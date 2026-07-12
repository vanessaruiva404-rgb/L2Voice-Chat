// voice.cpp — orchestrator. Wires AudioCapture → OpusEncoder →
// VoiceNetwork::SendProximityFrame on capture callbacks, and
// VoiceNetwork PacketCallback → OpusDecoder → AudioPlayback::Enqueue
// on incoming packets.
//
// Proximity routing and per-receiver gain/pan happen on the service
// (protocol rev 2). The client just plays whatever gain/pan the
// service stamps onto each packet.

#include "voice.h"
#include "apm.h"
#include "audio_io.h"
#include "opus_codec.h"
#include "overlay.h"
#include "voice_network.h"

#include <winsock2.h>      // must precede iphlpapi.h
#include <windows.h>
#include <iphlpapi.h>
#pragma comment(lib, "iphlpapi.lib")
#pragma comment(lib, "ws2_32.lib")

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>
#include <string>

namespace voice {

namespace {

static int64_t NowMillis() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

std::unordered_map<uint32_t, bool>  g_local_muted;
std::unordered_map<uint32_t, float> g_local_volume;
std::unordered_map<std::string, int>     g_speaker_channel;
std::unordered_map<std::string, int64_t> g_speaker_last_time;
std::mutex                          g_local_prefs_mu;

static void SanitizeName(const char* src, char* dst, size_t maxLen) {
    size_t dIdx = 0;
    for (size_t sIdx = 0; src[sIdx] != '\0' && dIdx < maxLen - 1; ++sIdx) {
        char c = src[sIdx];
        if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) {
            dst[dIdx++] = c;
        }
    }
    dst[dIdx] = '\0';
}

// Writes the current speaking state to l2voice_speak.ini (next to voice.ini).
// Must be called while holding g_local_prefs_mu.
// Strategy: ACTIVE speakers → write channel number (0-4).
//           EXPIRED speakers → explicitly write "-1" to the INI.
//           This ensures Unreal's FConfigCache reads the new value and updates memory,
//           avoiding the stale cache bug that happens when a key is simply deleted.
static void WriteVoiceSpeakIni(const wchar_t* ini_path) {
    if (!ini_path || ini_path[0] == 0) return;

    // Build path: same directory as voice.ini, but named l2voice_speak.ini
    char speak_path[MAX_PATH];
    size_t dummy = 0;
    wcstombs_s(&dummy, speak_path, ini_path, MAX_PATH - 1);
    
    // Find the filename portion and replace it
    char* lastSlash = strrchr(speak_path, '\\');
    if (!lastSlash) return;
    strcpy_s(lastSlash + 1, MAX_PATH - (lastSlash - speak_path) - 1, "l2voice_speak.ini");

    int64_t now = NowMillis();
    std::vector<std::string> expired_keys;

    // Write each active speaker (within 600ms window) to the file
    for (auto& kv : g_speaker_last_time) {
        char clean_name[64];
        SanitizeName(kv.first.c_str(), clean_name, sizeof(clean_name));

        if (now - kv.second < 600) {
            // Speaker is active: write channel number
            auto chIt = g_speaker_channel.find(kv.first);
            int ch = (chIt != g_speaker_channel.end()) ? chIt->second : 0;
            char val[8];
            sprintf_s(val, "%d", ch);
            WritePrivateProfileStringA("VoiceSpeak", clean_name, val, speak_path);
            Logf("[l2voice] WriteVoiceSpeakIni: %s (%s) = %s (elapsed=%lldms)\n", kv.first.c_str(), clean_name, val, (long long)(now - kv.second));
        } else {
            // Speaker expired: write 255 (invalid channel) to force UE2 FConfigCache update.
            // We use 255 instead of -1 because GetINIInt in UE2 cannot parse negative
            // integers — it returns 0 when it sees '-', making it look like proximity channel!
            WritePrivateProfileStringA("VoiceSpeak", clean_name, "255", speak_path);
            Logf("[l2voice] WriteVoiceSpeakIni (EXPIRED): %s (%s) key set to 255 (elapsed=%lldms)\n", kv.first.c_str(), clean_name, (long long)(now - kv.second));
            expired_keys.push_back(kv.first);
        }
    }

    // Clean up internal speaker maps for expired keys
    for (const auto& key : expired_keys) {
        g_speaker_last_time.erase(key);
        g_speaker_channel.erase(key);
    }

    // Flush the Windows INI cache to disk immediately
    WritePrivateProfileStringA(nullptr, nullptr, nullptr, speak_path);
}



// Per-channel listening prefs. Defaults: all enabled, all at 1.0.
// Loaded from voice.ini on init (keys ch_enabled_N / ch_volume_N),
// pushed to the voice-service on connect so it has the latest state.
//
// active_tx_channel is the destination the single PTT key (and the
// always-on mode) transmits on. The legacy per-channel PTT keys
// (ptt_party / ptt_clan / ptt_ally) still work — when held they
// override active_tx_channel for that instant.
struct ChannelPrefs {
    bool  enabled[4]  = { true, true, true, true };
    float volume[4]   = { 1.0f, 1.0f, 1.0f, 1.0f };
    int   active_tx_channel = 0;   // 0=Proximity, 1=Party, 2=Clan, 3=Ally, 4=CC
};

struct Mod {
    Config cfg{};
    ChannelPrefs ch_prefs;
    Apm apm;
    std::atomic<bool> running{false};
    // Path to voice.ini next to the DLL — captured at Init so the
    // setters can write changes back without re-resolving each time.
    wchar_t ini_path[MAX_PATH] = {};

    std::atomic<uint16_t> tx_seq{0};

    AudioCapture capture;
    AudioPlayback playback;
    VoiceOpusEncoder encoder;
    VoiceNetwork  net;
    std::thread   keepalive_thread;

    std::mutex dec_mu;
    std::unordered_map<uint32_t, std::unique_ptr<VoiceOpusDecoder>> decoders;

    VoiceOpusDecoder* DecoderFor(uint32_t src) {
        std::lock_guard<std::mutex> lk(dec_mu);
        auto it = decoders.find(src);
        if (it != decoders.end()) return it->second.get();
        auto d = std::make_unique<VoiceOpusDecoder>();
        if (!d->Init()) return nullptr;
        VoiceOpusDecoder* p = d.get();
        decoders.emplace(src, std::move(d));
        return p;
    }
};

Mod g_mod;

uint32_t GetPlayerIdByName(const char* name);
std::atomic<bool> g_prioritize_clan_leader{false};
std::atomic<bool> g_prioritize_party_leader{false};

// True iff a window owned by THIS process currently has keyboard focus.
// Used to gate microphone capture so holding the PTT key while the L2
// window is minimized / Alt-Tab'd doesn't transmit anything.
bool L2HasForegroundFocus() {
    HWND fg = GetForegroundWindow();
    if (!fg) return false;
    DWORD fgPid = 0;
    GetWindowThreadProcessId(fg, &fgPid);
    return fgPid == GetCurrentProcessId();
}

void OnCaptureFrame(const int16_t* pcm, uint32_t samples) {
    if (!g_mod.running.load()) return;
    if (samples != kFrameSamples) return;

    // Run APM on capture frame first to keep AEC and denoisers warm,
    // and to perform VAD (Voice Activity Detection).
    int16_t apm_pcm[kFrameSamples];
    std::memcpy(apm_pcm, pcm, samples * sizeof(int16_t));
    int16_t aec_ref[kFrameSamples];
    g_mod.playback.PopPlaybackReference(aec_ref, kFrameSamples);
    
    g_mod.apm.ProcessFrame(apm_pcm, samples, aec_ref);
    bool is_speaking = g_mod.apm.IsSpeaking();

    bool focused = !g_mod.cfg.require_focus || L2HasForegroundFocus();

    auto held = [](int vk) -> bool {
        return vk != 0 && (GetAsyncKeyState(vk) & 0x8000) != 0;
    };

    uint8_t channel = 0xFF;   // none
    bool ptt_held = false;

    // Legacy explicit-key path still wins when held — power users keep
    // multiple PTTs configured. Otherwise the main PTT (and always-on)
    // route to the user's selected active TX channel.
    if (held(g_mod.cfg.ptt_party)) {
        channel = 1;
        ptt_held = true;
    } else if (held(g_mod.cfg.ptt_clan)) {
        channel = 2;
        ptt_held = true;
    } else if (held(g_mod.cfg.ptt_ally)) {
        channel = 3;
        ptt_held = true;
    } else if (held(g_mod.cfg.ptt_proximity)) {
        int sel = g_mod.ch_prefs.active_tx_channel;
        if (sel < 0 || sel > 4) sel = 0;
        // Mode auto-redirect (Prompt §Regra 6)
        if (sel == 0 && g_mod.net.LocalClanMode() != 0) {
            sel = 2;
        }
        channel = (uint8_t)sel;
        ptt_held = true;
    } else if (g_mod.cfg.always_on) {
        int sel = g_mod.ch_prefs.active_tx_channel;
        if (sel < 0 || sel > 4) sel = 0;
        if (sel == 0 && g_mod.net.LocalClanMode() != 0) {
            sel = 2;
        }
        channel = (uint8_t)sel;
    }

    // We only transmit if:
    // 1. Focused (or focus not required)
    // 2. Channel is valid
    // 3. EITHER a PTT key is held, OR always_on is enabled AND the user is actually speaking (VAD).
    bool transmit = focused && (channel != 0xFF) && (ptt_held || (g_mod.cfg.always_on && is_speaking));

    static uint32_t cap_frames = 0;
    if ((++cap_frames % 50) == 0) {
        char dbg[250];
        _snprintf_s(dbg, sizeof(dbg), _TRUNCATE,
            "[l2voice] capture=%u tx=%d ch=%u focus=%d on=%d spk=%d ws=%d sid=%u\n",
            cap_frames, transmit ? 1 : 0,
            channel == 0xFF ? 0xFF : channel,
            focused ? 1 : 0, g_mod.cfg.always_on ? 1 : 0,
            is_speaking ? 1 : 0,
            g_mod.net.IsConnected() ? 1 : 0, g_mod.net.SessionID());
        OutputDebugStringA(dbg);
    }

    char my_name[64] = {};
    if (g_mod.cfg.char_name[0]) {
        std::strncpy(my_name, g_mod.cfg.char_name, sizeof(my_name) - 1);
    } else if (g_mod.net.PlayerID() != 0) {
        GetPlayerName(g_mod.net.PlayerID(), my_name, sizeof(my_name));
    }

    if (transmit) {
        if (my_name[0]) {
            std::lock_guard<std::mutex> lk(g_local_prefs_mu);
            g_speaker_channel[my_name] = channel;
            g_speaker_last_time[my_name] = NowMillis();
            WriteVoiceSpeakIni(g_mod.ini_path);  // Update l2voice_speak.ini for UnrealScript to read
        }

        if (!g_mod.net.IsConnected()) return;

        Logf("[l2voice] OnCaptureFrame: starting transmit. channel=%u\n", (unsigned)channel);

        Logf("[l2voice] OnCaptureFrame: before Encode\n");
        uint8_t opus_buf[kMaxPacketBytes];
        int n = g_mod.encoder.Encode(apm_pcm, opus_buf, sizeof(opus_buf));
        Logf("[l2voice] OnCaptureFrame: after Encode n=%d\n", n);
        if (n <= 0) return;

        uint16_t seq = g_mod.tx_seq.fetch_add(1, std::memory_order_relaxed);
        Logf("[l2voice] OnCaptureFrame: before Send proximity/group channel=%u seq=%u\n", (unsigned)channel, (unsigned)seq);
        if (channel == 0) g_mod.net.SendProximityFrame(seq, opus_buf, n);
        else              g_mod.net.SendGroupFrame(channel, seq, opus_buf, n);
        Logf("[l2voice] OnCaptureFrame: transmit done\n");
    } else {
        if (my_name[0]) {
            std::lock_guard<std::mutex> lk(g_local_prefs_mu);
            auto it = g_speaker_last_time.find(my_name);
            if (it != g_speaker_last_time.end()) {
                // PTT released: immediately write -1 to the INI so Unreal's
                // FConfigCache gets a real value change (not a missing key).
                // Then remove from internal maps so WriteVoiceSpeakIni won't
                // re-add this player on the next periodic call.
                char speak_path[MAX_PATH];
                size_t dummy2 = 0;
                wcstombs_s(&dummy2, speak_path, g_mod.ini_path, MAX_PATH - 1);
                char* lastSlash2 = strrchr(speak_path, '\\');
                if (lastSlash2) {
                    strcpy_s(lastSlash2 + 1, MAX_PATH - (lastSlash2 - speak_path) - 1, "l2voice_speak.ini");
                    char clean_name[64];
                    SanitizeName(my_name, clean_name, sizeof(clean_name));
                    // Write 255 (not -1!) because UE2's GetINIInt cannot parse negative integers.
                    // It returns 0 for "-1" (sees '-' as invalid char), making UE2 think
                    // the player is speaking on channel 0 (proximity). 255 is always > 4 (max channel).
                    WritePrivateProfileStringA("VoiceSpeak", clean_name, "255", speak_path);
                    WritePrivateProfileStringA(nullptr, nullptr, nullptr, speak_path); // flush
                    Logf("[l2voice] PTT released: wrote 255 for %s (%s) directly\n", my_name, clean_name);
                }
                g_speaker_last_time.erase(it);
                g_speaker_channel.erase(my_name);
            }
        }
    }
}

void OnIncomingPacket(uint8_t channel, uint32_t src, uint16_t /*seq*/,
                      uint8_t gain_u8, int8_t pan_i8,
                      const uint8_t* opus_payload, uint16_t opus_len) {
    // Make sure we have a name on file for the speaker so the overlay
    // can label them. Cheap — VoiceNetwork dedupes inflight queries
    // and cached names skip the WS roundtrip.
    g_mod.net.SendNameQuery(src);

    static uint32_t rx = 0;
    if ((++rx % 50) == 1) {
        char dbg[160];
        _snprintf_s(dbg, sizeof(dbg), _TRUNCATE,
            "[l2voice] recv #%u ch=%u src=%u gain=%u pan=%d opus=%u\n",
            rx, channel, src, gain_u8, pan_i8, opus_len);
        OutputDebugStringA(dbg);
    }

    Logf("[l2voice] OnIncomingPacket: ch=%u src=%u len=%u\n", (unsigned)channel, src, (unsigned)opus_len);

    // Channel 0 = proximity (server-stamped gain/pan).
    // Channels 1..4 = group voice (party/clan/ally/CC). The server's
    // router pre-applies channel volume + per-player volume into gain
    // (1.0 → 128). Pan=0 always.
    // Channel 8 = unified-mode rewrite of CLAN/ALLY when a clan mode is
    // active. Mixed at full volume with override (ForceAudible).
    uint8_t prefSlot = channel;
    if (channel == 4) prefSlot = 2; // CC uses CLAN slot
    if (prefSlot < 4) {
        if (!g_mod.ch_prefs.enabled[prefSlot]) {
            return;
        }
    }

    // 1. Prioritize Clan Leader filter
    if (g_prioritize_clan_leader.load()) {
        if (channel == 2 || channel == 3 || channel == 8) {
            uint32_t clanLeaderId = 0;
            OverlayMember clanRoster[64];
            size_t clanCount = GetGroupRoster(2, clanRoster, 64);
            for (size_t i = 0; i < clanCount; ++i) {
                if (clanRoster[i].clan_role == 2) {
                    clanLeaderId = clanRoster[i].player_id;
                    break;
                }
            }
            char spName[64];
            if (GetSpeakerName(src, spName, sizeof(spName)) && spName[0]) {
                uint32_t pid = GetPlayerIdByName(spName);
                if (pid != clanLeaderId) {
                    return; // Mute everyone except Clan Leader!
                }
            }
        }
    }

    // 2. Prioritize Party Leader filter
    if (g_prioritize_party_leader.load()) {
        if (channel == 1) {
            uint32_t partyLeaderId = 0;
            OverlayMember partyRoster[64];
            size_t partyCount = GetGroupRoster(1, partyRoster, 64);
            if (partyCount > 0) {
                partyLeaderId = partyRoster[0].player_id;
            }
            char spName[64];
            if (GetSpeakerName(src, spName, sizeof(spName)) && spName[0]) {
                uint32_t pid = GetPlayerIdByName(spName);
                if (pid != partyLeaderId) {
                    return; // Mute everyone except Party Leader!
                }
            }
        }
    }

    VoiceOpusDecoder* dec = g_mod.DecoderFor(src);
    if (!dec) {
        Logf("[l2voice] OnIncomingPacket: DecoderFor returned null\n");
        return;
    }

    int16_t pcm[kFrameSamples];
    Logf("[l2voice] OnIncomingPacket: before Decode\n");
    int got = dec->Decode(opus_payload, opus_len, pcm, kFrameSamples);
    Logf("[l2voice] OnIncomingPacket: after Decode got=%d\n", got);
    if (got <= 0) return;

    float gain = (float)gain_u8 / 255.0f;
    float pan  = (float)pan_i8  / 127.0f;

    if (prefSlot < 4) {
        gain *= g_mod.ch_prefs.volume[prefSlot];
    }

    char spName[64];
    if (GetSpeakerName(src, spName, sizeof(spName)) && spName[0]) {
        {
            std::lock_guard<std::mutex> lk(g_local_prefs_mu);
            g_speaker_channel[spName] = channel;
            g_speaker_last_time[spName] = NowMillis();
            WriteVoiceSpeakIni(g_mod.ini_path);  // Update l2voice_speak.ini for UnrealScript to read
        }
        uint32_t pid = GetPlayerIdByName(spName);
        if (pid != 0) {
            if (IsPlayerMuted(pid)) {
                gain = 0.0f;
            } else {
                gain *= GetPlayerVolume(pid);
            }
        }
    }

    Logf("[l2voice] OnIncomingPacket: before Enqueue got=%d gain=%.3f pan=%.3f\n", got, gain, pan);
    g_mod.playback.Enqueue(src, pcm, (uint32_t)got, gain, pan);
    Logf("[l2voice] OnIncomingPacket: Enqueue done\n");
}

uint32_t GetPlayerIdByName(const char* name) {
    if (!name || !name[0]) return 0;
    for (uint8_t g = 1; g <= 4; ++g) {
        OverlayMember roster[64];
        size_t count = GetGroupRoster(g, roster, 64);
        for (size_t i = 0; i < count; ++i) {
            char memName[64];
            if (GetPlayerName(roster[i].player_id, memName, sizeof(memName)) && _stricmp(memName, name) == 0) {
                return roster[i].player_id;
            }
        }
    }
    return 0;
}

void UpdateActiveSpeakerLocalMuteVolume(uint32_t player_id) {
    char memName[64];
    if (!GetPlayerName(player_id, memName, sizeof(memName)) || !memName[0]) return;
    
    SpeakerInfo speakers[64];
    size_t count = 0;
    g_mod.playback.GetSpeakerInfos(speakers, 64, count);
    for (size_t i = 0; i < count; ++i) {
        char spName[64];
        if (GetSpeakerName(speakers[i].src_id, spName, sizeof(spName)) && _stricmp(spName, memName) == 0) {
            bool muted = IsPlayerMuted(player_id);
            float vol = GetPlayerVolume(player_id);
            g_mod.playback.SetSourceMuted(speakers[i].src_id, muted);
            g_mod.playback.SetSourceVolume(speakers[i].src_id, muted ? 0.0f : vol);
        }
    }
}

}  // namespace

Config DefaultConfig() {
    Config c{};
    std::strncpy(c.ws_url, "ws://127.0.0.1:17667/ws", sizeof(c.ws_url) - 1);
    c.udp_host[0]   = 0;
    c.udp_port      = 0;
    c.capture_device[0] = 0;
    c.playback_device[0] = 0;
    c.min_dist_cm   = 500.0f;
    c.max_dist_cm   = 2500.0f;
    c.ptt_proximity = 'H';  // V conflicts with the client's inventory hotkey
    c.ptt_party     = 'B';
    c.ptt_clan      = 'N';
    c.ptt_ally      = 'M';
    c.enabled       = true;
    c.auto_connect  = true;
    c.require_focus = true;
    c.always_on     = true;
    c.master_volume = 1.0f;
    c.char_name[0]  = 0;
    return c;
}


// Enumerates this process's owned TCP connections and returns the local
// (source) ports. The bridge will match these against L2GameClient
// remote ports to figure out which player this DLL belongs to.
//
// We skip the loopback (127.0.0.1) side and the WS port itself so the
// list doesn't include the voice-server connection. Everything else is
// fair game — bridge knows which port is the game socket.
std::vector<uint16_t> EnumerateOwnTcpPorts(uint16_t exclude_remote_port) {
    std::vector<uint16_t> out;
    DWORD pid = GetCurrentProcessId();
    DWORD size = 0;
    GetExtendedTcpTable(nullptr, &size, FALSE, AF_INET,
                        TCP_TABLE_OWNER_PID_ALL, 0);
    if (size == 0) return out;
    std::vector<uint8_t> buf(size);
    if (GetExtendedTcpTable(buf.data(), &size, FALSE, AF_INET,
                            TCP_TABLE_OWNER_PID_ALL, 0) != NO_ERROR) {
        return out;
    }
    auto* tbl = reinterpret_cast<MIB_TCPTABLE_OWNER_PID*>(buf.data());
    for (DWORD i = 0; i < tbl->dwNumEntries; ++i) {
        const MIB_TCPROW_OWNER_PID& r = tbl->table[i];
        if (r.dwOwningPid != pid) continue;
        uint16_t localPort  = ntohs((u_short)r.dwLocalPort);
        uint16_t remotePort = ntohs((u_short)r.dwRemotePort);
        if (remotePort == exclude_remote_port) continue;
        if (remotePort == 9014) continue; // Exclude Lineage II Login Server port to avoid false in-world detection
        // Only ESTABLISHED connections — listening sockets don't have
        // a meaningful "remote" the GS knows about.
        if (r.dwState != MIB_TCP_STATE_ESTAB) continue;
        out.push_back(localPort);
    }
    return out;
}

bool LoadConfigFromIni(const wchar_t* path, Config* out) {
    if (!path || !out) return false;
    Config c = DefaultConfig();
    wchar_t buf[512];
    auto getS = [&](const wchar_t* key, char* dst, size_t cap, const char* def) {
        GetPrivateProfileStringW(L"voice", key, L"", buf, 512, path);
        if (buf[0] == 0) { std::strncpy(dst, def, cap - 1); return; }
        size_t n = 0;
        wcstombs_s(&n, dst, cap, buf, cap - 1);
    };
    auto getI = [&](const wchar_t* key, int def) -> int {
        return GetPrivateProfileIntW(L"voice", key, def, path);
    };
    getS(L"ws_url", c.ws_url, sizeof(c.ws_url), c.ws_url);
    getS(L"capture_device", c.capture_device, sizeof(c.capture_device), "");
    getS(L"playback_device", c.playback_device, sizeof(c.playback_device), "");
    getS(L"char_name", c.char_name, sizeof(c.char_name), "");
    c.min_dist_cm    = (float)getI(L"min_dist_cm", (int)c.min_dist_cm);
    c.max_dist_cm    = (float)getI(L"max_dist_cm", (int)c.max_dist_cm);
    c.ptt_proximity  = getI(L"ptt_proximity",  c.ptt_proximity);
    c.ptt_party      = getI(L"ptt_party",      c.ptt_party);
    c.ptt_clan       = getI(L"ptt_clan",       c.ptt_clan);
    c.ptt_ally       = getI(L"ptt_ally",       c.ptt_ally);
    c.enabled        = getI(L"enabled", 1) != 0;
    c.auto_connect   = getI(L"auto_connect", 1) != 0;
    c.require_focus  = getI(L"require_focus", 1) != 0;
    c.always_on      = getI(L"always_on", 1) != 0;
    c.master_volume  = (float)getI(L"master_volume", 100) / 100.0f;
    *out = c;
    return true;
}

void RefreshClientPorts();   // fwd decl

// Used by dllmain.cpp to tell us where voice.ini lives (so the
// overlay setters can persist changes). Call once before Init.
void SetIniPath(const wchar_t* path) {
    if (!path) { g_mod.ini_path[0] = 0; return; }
    wcsncpy_s(g_mod.ini_path, MAX_PATH, path, MAX_PATH - 1);
}

// Writes an integer key to [voice] in voice.ini. Cheap: WinAPI does
// the parse/replace/write internally.
static void IniWriteInt(const wchar_t* key, int value) {
    if (g_mod.ini_path[0] == 0) return;
    wchar_t buf[32];
    swprintf_s(buf, L"%d", value);
    WritePrivateProfileStringW(L"voice", key, buf, g_mod.ini_path);
}

static int IniReadInt(const wchar_t* key, int fallback) {
    if (g_mod.ini_path[0] == 0) return fallback;
    return (int)GetPrivateProfileIntW(L"voice", key, fallback, g_mod.ini_path);
}

// Restores per-channel prefs from voice.ini. Called once during Init
// AFTER ini_path is set. Defaults preserved when keys are absent.
static void LoadChannelPrefs() {
    wchar_t key[24];
    for (int i = 0; i < 4; ++i) {
        swprintf_s(key, L"ch_enabled_%d", i);
        g_mod.ch_prefs.enabled[i] = IniReadInt(key, 1) != 0;
        swprintf_s(key, L"ch_volume_%d", i);
        int pct = IniReadInt(key, 100);
        if (pct < 0) pct = 0;
        if (pct > 200) pct = 200;
        g_mod.ch_prefs.volume[i] = pct / 100.0f;
    }
    int sel = IniReadInt(L"active_tx_channel", 0);
    if (sel < 0 || sel > 4) sel = 0;
    g_mod.ch_prefs.active_tx_channel = sel;
}

bool Init(const Config& cfg) {
    if (g_mod.running.load()) return true;
    if (!cfg.enabled)         return false;

    g_mod.cfg = cfg;
    LoadChannelPrefs();

        // Clear the VoiceSpeak section of l2voice_speak.ini on initialization to prevent stale speakers from previous sessions
    if (g_mod.ini_path[0] != 0) {
        char speak_path[MAX_PATH];
        size_t dummy = 0;
        wcstombs_s(&dummy, speak_path, g_mod.ini_path, MAX_PATH - 1);
        char* lastSlash = strrchr(speak_path, '\\');
        if (lastSlash) {
            strcpy_s(lastSlash + 1, MAX_PATH - (lastSlash - speak_path) - 1, "l2voice_speak.ini");
            WritePrivateProfileStringA("VoiceSpeak", nullptr, nullptr, speak_path);
            WritePrivateProfileStringA(nullptr, nullptr, nullptr, speak_path); // flush
        }
    }

    // APM config — exposed in voice.ini under [voice].apm_*. Defaults
    // match ApmConfig's defaults (AEC+NS+AGC+HPF on).
    ApmConfig apmCfg;
    apmCfg.aec_enabled = IniReadInt(L"apm_aec",  apmCfg.aec_enabled  ? 1 : 0) != 0;
    apmCfg.hpf_enabled = IniReadInt(L"apm_hpf",  apmCfg.hpf_enabled  ? 1 : 0) != 0;
    apmCfg.ns_enabled  = IniReadInt(L"apm_ns",   apmCfg.ns_enabled   ? 1 : 0) != 0;
    apmCfg.agc_enabled = IniReadInt(L"apm_agc",  apmCfg.agc_enabled  ? 1 : 0) != 0;
    Logf("[l2voice] Init: APM configs loaded: aec=%d hpf=%d ns=%d agc=%d\n",
         apmCfg.aec_enabled ? 1 : 0,
         apmCfg.hpf_enabled ? 1 : 0,
         apmCfg.ns_enabled ? 1 : 0,
         apmCfg.agc_enabled ? 1 : 0);
    g_mod.apm.Configure(apmCfg);

    if (!g_mod.encoder.Init())                       return false;
    if (!g_mod.playback.Start(cfg.playback_device))  return false;
    g_mod.playback.SetMasterVolume(cfg.master_volume);

    if (!g_mod.capture.Start(cfg.capture_device, &OnCaptureFrame)) {
        g_mod.playback.Stop();
        return false;
    }

    if (cfg.auto_connect) {
        // On every WS open (initial + each reconnect), refresh the
        // TCP port snapshot synchronously so the immediate TrySendAuth
        // that follows already has the latest data. Without this hook
        // the DLL had to wait for the next periodic render-frame poll
        // (up to ~250ms after the fix above, but much longer during
        // a low-FPS loading screen).
        g_mod.net.SetWsOpenHook([]() {
            RefreshClientPorts();
        });
        g_mod.net.Start(cfg.ws_url,
                        [](uint32_t /*sid*/, const char*, uint16_t) {
                            // On auth, push our cached per-channel prefs
                            // so the server's PlayerPrefs reflects local
                            // state from voice.ini (defaults are all-on
                            // server-side, but user may have tweaked).
                            for (int i = 0; i < 4; ++i) {
                                g_mod.net.SendSetChannelEnabled(
                                    (uint8_t)i, g_mod.ch_prefs.enabled[i]);
                                g_mod.net.SendSetChannelVolume(
                                    (uint8_t)i, g_mod.ch_prefs.volume[i]);
                            }
                        },
                        &OnIncomingPacket);
        // Identity is resolved server-side via TCP source-port matching.
        // The bridge correlates this DLL's local-port list against
        // L2GameClient remote ports → returns the player_id. We push an
        // initial snapshot here and refresh on each WS reconnect (see
        // OnRenderFrame).
        RefreshClientPorts();
    }

    g_mod.running.store(true);

    // Install the in-game overlay (D3D9 EndScene hook + ImGui panel).
    // Delaying this to OnRenderFrame to prevent race conditions during game startup D3D9 init.
    /*
    if (!InstallOverlay()) {
        OutputDebugStringA("[l2voice] overlay install failed; continuing without UI\n");
    }
    */

    // Keepalive thread: every 5s the DLL emits a UDP header-only
    // packet so the voice-service learns (and refreshes) our UDP
    // source address. WITHOUT this, the service only knows the UDP
    // addrs of clients that are actively transmitting audio — so
    // listeners-who-haven't-spoken-yet never receive anything.
    g_mod.keepalive_thread = std::thread([] {
        using namespace std::chrono;
        while (g_mod.running.load(std::memory_order_acquire)) {
            for (int i = 0; i < 10 && g_mod.running.load(); ++i) {
                std::this_thread::sleep_for(milliseconds(500));
            }
            if (g_mod.net.IsConnected() && g_mod.net.SessionID() != 0) {
                g_mod.net.SendKeepalive();
            }
        }
    });
    return true;
}

// Tracks in-world vs out-of-world transitions so we can suspend the
// voice WS while the player is at char-select / disconnected. Updated
// inside RefreshClientPorts; consumed by OnRenderFrame.
static size_t   g_last_port_count  = 0;
static int64_t  g_out_of_world_at  = 0;  // ms timestamp when port count last dropped to 0
static bool     g_voice_suspended  = false;
constexpr int64_t kOutOfWorldGraceMs = 5000;

void RefreshClientPorts() {
    // Exclude the WS connection itself (port 17667 by default). We
    // could parse cfg.ws_url for the actual port, but 17667 is the
    // default and adding a few extras to the list is harmless.
    auto ports = EnumerateOwnTcpPorts(/*exclude_remote_port*/ 17667);
    char dbg[128];
    _snprintf_s(dbg, sizeof(dbg), _TRUNCATE,
        "[l2voice] RefreshClientPorts: %zu connections\n", ports.size());
    OutputDebugStringA(dbg);

    // Transition detection — used by OnRenderFrame to suspend/resume.
    if (g_last_port_count == 0 && !ports.empty()) {
        // Just entered world. Reset out-of-world timer; OnRenderFrame
        // will resume the WS if it was suspended.
        g_out_of_world_at = 0;
        char buf[128];
        _snprintf_s(buf, sizeof(buf), _TRUNCATE,
            "[l2voice] in-world detected (%zu port(s))\n", ports.size());
        OutputDebugStringA(buf);
    } else if (g_last_port_count > 0 && ports.empty()) {
        // Just left world (char-select / DC / logout). Start grace.
        g_out_of_world_at = NowMillis();
        char buf[128];
        _snprintf_s(buf, sizeof(buf), _TRUNCATE,
            "[l2voice] out-of-world detected; will suspend in %lldms\n",
            (long long)kOutOfWorldGraceMs);
        OutputDebugStringA(buf);
    }
    g_last_port_count = ports.size();

    if (ports.empty()) return;
    g_mod.net.SetClientPorts(ports.data(), ports.size(), g_mod.cfg.char_name);
}

void Shutdown() {
    if (!g_mod.running.exchange(false)) return;
    UninstallOverlay();
    if (g_mod.keepalive_thread.joinable()) g_mod.keepalive_thread.detach();
    g_mod.capture.Stop();
    g_mod.net.Stop();
    g_mod.playback.Stop();
    std::lock_guard<std::mutex> lk(g_mod.dec_mu);
    g_mod.decoders.clear();
}

OverlayState SnapshotOverlayState() {
    OverlayState s{};
    s.ws_connected     = g_mod.net.IsConnected();
    s.session_id       = g_mod.net.SessionID();
    s.player_id        = g_mod.net.PlayerID();
    s.active_speakers  = g_mod.playback.ActiveSpeakers();
    s.require_focus    = g_mod.cfg.require_focus;
    s.always_on        = g_mod.cfg.always_on;
    s.ptt_proximity_vk = g_mod.cfg.ptt_proximity;
    s.master_volume    = g_mod.playback.GetMasterVolume();
    for (int i = 0; i < 4; ++i) {
        s.ch_enabled[i] = g_mod.ch_prefs.enabled[i];
        s.ch_volume[i]  = g_mod.ch_prefs.volume[i];
    }
    std::strncpy(s.char_name, g_mod.cfg.char_name, sizeof(s.char_name) - 1);
    s.char_name[sizeof(s.char_name) - 1] = 0;
    return s;
}

// Setters persist to voice.ini so the user's preferences survive
// the next L2 launch. Master volume is stored as a percent (0..200).
void SetRequireFocus(bool v) {
    g_mod.cfg.require_focus = v;
    IniWriteInt(L"require_focus", v ? 1 : 0);
}
void SetAlwaysOn(bool v) {
    g_mod.cfg.always_on = v;
    IniWriteInt(L"always_on", v ? 1 : 0);
}
void SetPttProximityVk(int vk) {
    g_mod.cfg.ptt_proximity = vk;
    IniWriteInt(L"ptt_proximity", vk);
}
void SetPttPartyVk(int vk) {
    g_mod.cfg.ptt_party = vk;
    IniWriteInt(L"ptt_party", vk);
}
void SetPttClanVk(int vk) {
    g_mod.cfg.ptt_clan = vk;
    IniWriteInt(L"ptt_clan", vk);
}
void SetPttAllyVk(int vk) {
    g_mod.cfg.ptt_ally = vk;
    IniWriteInt(L"ptt_ally", vk);
}
int GetPttPartyVk() { return g_mod.cfg.ptt_party; }
int GetPttClanVk()  { return g_mod.cfg.ptt_clan; }
int GetPttAllyVk()  { return g_mod.cfg.ptt_ally; }
void SetMasterVolume(float g) {
    g_mod.playback.SetMasterVolume(g);
    IniWriteInt(L"master_volume", (int)(g * 100.0f + 0.5f));
}

// Per-channel prefs: update local cache, persist to voice.ini under
// keys ch_enabled_<n> / ch_volume_<n>, and forward to voice-service so
// its router sees the new value.
//
// `channel` is 0..3; out-of-range silently no-ops.
void SetChannelEnabled(int channel, bool enabled) {
    if (channel < 0 || channel > 3) return;
    g_mod.ch_prefs.enabled[channel] = enabled;
    wchar_t key[24];
    _snwprintf_s(key, _TRUNCATE, L"ch_enabled_%d", channel);
    IniWriteInt(key, enabled ? 1 : 0);
    g_mod.net.SendSetChannelEnabled((uint8_t)channel, enabled);
}

void SetChannelVolume(int channel, float volume) {
    if (channel < 0 || channel > 3) return;
    if (volume < 0) volume = 0;
    if (volume > 2) volume = 2;
    g_mod.ch_prefs.volume[channel] = volume;
    wchar_t key[24];
    _snwprintf_s(key, _TRUNCATE, L"ch_volume_%d", channel);
    IniWriteInt(key, (int)(volume * 100.0f + 0.5f));
    g_mod.net.SendSetChannelVolume((uint8_t)channel, volume);
}

// Sets the channel that PTT (and always-on) transmits on. 0..4 valid.
// Persisted locally — the server doesn't need to know, it just routes
// whatever channel byte appears in the ingress packet header.
void SetActiveTxChannel(int channel) {
    if (channel < 0 || channel > 4) return;
    g_mod.ch_prefs.active_tx_channel = channel;
    IniWriteInt(L"active_tx_channel", channel);
}

int GetActiveTxChannel() {
    return g_mod.ch_prefs.active_tx_channel;
}
void GetSpeakerList(SpeakerInfo* out, size_t cap, size_t& count) {
    g_mod.playback.GetSpeakerInfos(out, cap, count);
}
void SetSpeakerMuted(uint32_t src_id, bool muted) {
    g_mod.playback.SetSourceMuted(src_id, muted);
}

void SetSpeakerVolume(uint32_t src_id, float volume) {
    g_mod.playback.SetSourceVolume(src_id, volume);
}

bool GetSpeakerName(uint32_t src_id, char* out, size_t cap) {
    // Side-effect: kick off a query if we don't have it yet.
    g_mod.net.SendNameQuery(src_id);
    return g_mod.net.CachedName(src_id, out, cap);
}

size_t GetGroupRoster(uint8_t group, OverlayMember* out, size_t cap) {
    static_assert(sizeof(OverlayMember) == sizeof(VoiceNetwork::GroupMember),
        "OverlayMember and GroupMember must have identical layout");
    return g_mod.net.GetGroupMembers(group,
        reinterpret_cast<VoiceNetwork::GroupMember*>(out), cap);
}

bool GetPlayerName(uint32_t player_id, char* out, size_t cap) {
    g_mod.net.SendPlayerNameQuery(player_id);
    return g_mod.net.CachedPlayerName(player_id, out, cap);
}

uint32_t GetCCID()           { return g_mod.net.LocalCCID(); }
uint32_t GetCCLeaderID()     { return g_mod.net.LocalCCLeaderID(); }
bool     GetCCCanSpeak()     { return g_mod.net.LocalCCCanSpeak(); }
uint8_t  GetLocalRole()      { return g_mod.net.LocalRole(); }
uint8_t  GetLocalClanMode()  { return g_mod.net.LocalClanMode(); }

size_t GetActiveToasts(OverlayToast* out, size_t cap) {
    static_assert(sizeof(OverlayToast) == sizeof(VoiceNetwork::Toast),
        "OverlayToast and VoiceNetwork::Toast must share layout");
    return g_mod.net.GetToasts(
        reinterpret_cast<VoiceNetwork::Toast*>(out), cap);
}

void SendCCGrantSpeak(uint32_t target_player_id, bool granted) {
    g_mod.net.SendCCGrantSpeak(target_player_id, granted);
}
void SendClanRemoteMute(uint32_t target_player_id, bool muted, const char* scope) {
    g_mod.net.SendClanRemoteMute(target_player_id, muted, scope);
}
void SendClanSetMode(uint8_t mode) {
    g_mod.net.SendClanSetMode(mode);
}
void SendClanPromoteSub(uint32_t target_player_id, bool promote) {
    g_mod.net.SendClanPromoteSubLeader(target_player_id, promote);
}

void OnRenderFrame() {
    // Refresh the TCP-port list periodically — by the time the user
    // is in-world, their L2 client has opened the GS socket; before
    // that there's nothing useful to send. Cheap (one syscall).
    // Poll the L2 TCP table every 15 render frames (~250ms at 60 fps;
    // ~5s during a low-fps loading screen). Faster than the original
    // 1s poll so the voice auth fires sooner after the player enters
    // the world. The on-ws-open hook below covers reconnect cases.
    static int counter = 0;
    if ((counter++ % 15) == 0) RefreshClientPorts();
    // WS ping for round-trip latency. SendPingTick already self-throttles
    // to once per ~2 s, so calling each frame is fine.
    g_mod.net.SendPingTick();

    // Auto-suspend the voice WS when the L2 client has been at
    // char-select / disconnected / logout for the grace window. The
    // server-side session is otherwise dropped on player_logout but
    // the WS stays open, holding a TCP slot for nothing. When the
    // player re-enters world, ports return → resume.
    int64_t now_ms = NowMillis();

    if (!g_voice_suspended
        && g_out_of_world_at > 0
        && (now_ms - g_out_of_world_at) > kOutOfWorldGraceMs) {
        char buf[160];
        _snprintf_s(buf, sizeof(buf), _TRUNCATE,
            "[l2voice] suspending voice WS — player out of world for %lldms\n",
            (long long)(now_ms - g_out_of_world_at));
        OutputDebugStringA(buf);
        g_mod.net.SuspendWs();
        g_voice_suspended = true;
    }
    if (g_voice_suspended && g_last_port_count > 0) {
        OutputDebugStringA("[l2voice] resuming voice WS — player back in world\n");
        g_mod.net.ResumeWs();
        g_voice_suspended = false;
    }

    // Periodic speech cleanup: check every 100ms if there are cached speakers to expire
    static int64_t last_speak_cleanup = 0;
    if (now_ms - last_speak_cleanup >= 100) {
        last_speak_cleanup = now_ms;
        std::lock_guard<std::mutex> lk(g_local_prefs_mu);
        if (!g_speaker_last_time.empty()) {
            WriteVoiceSpeakIni(g_mod.ini_path);
        }
    }
}

int GetVoicePingMs() {
    return g_mod.net.LastPingMs();
}

void GetMiniListPos(int* x, int* y) {
    if (x) *x = IniReadInt(L"mini_list_x", 0);
    if (y) *y = IniReadInt(L"mini_list_y", 0);
}
void SaveMiniListPos(int x, int y) {
    IniWriteInt(L"mini_list_x", x);
    IniWriteInt(L"mini_list_y", y);
}
void GetMicIconPos(int* x, int* y) {
    if (x) *x = IniReadInt(L"mic_icon_x", 0);
    if (y) *y = IniReadInt(L"mic_icon_y", 0);
}
void SaveMicIconPos(int x, int y) {
    IniWriteInt(L"mic_icon_x", x);
    IniWriteInt(L"mic_icon_y", y);
}

void SetCaptureDevice(const char* name) {
    if (!name) return;
    std::strncpy(g_mod.cfg.capture_device, name, sizeof(g_mod.cfg.capture_device) - 1);
    g_mod.cfg.capture_device[sizeof(g_mod.cfg.capture_device) - 1] = 0;
    
    if (g_mod.ini_path[0] != 0) {
        wchar_t wName[128] = {0};
        size_t converted = 0;
        mbstowcs_s(&converted, wName, name, 127);
        WritePrivateProfileStringW(L"voice", L"capture_device", wName, g_mod.ini_path);
    }
    
    if (g_mod.capture.IsRunning()) {
        g_mod.capture.Stop();
        g_mod.capture.Start(g_mod.cfg.capture_device, &OnCaptureFrame);
    }
}

void SetPlaybackDevice(const char* name) {
    if (!name) return;
    std::strncpy(g_mod.cfg.playback_device, name, sizeof(g_mod.cfg.playback_device) - 1);
    g_mod.cfg.playback_device[sizeof(g_mod.cfg.playback_device) - 1] = 0;
    
    if (g_mod.ini_path[0] != 0) {
        wchar_t wName[128] = {0};
        size_t converted = 0;
        mbstowcs_s(&converted, wName, name, 127);
        WritePrivateProfileStringW(L"voice", L"playback_device", wName, g_mod.ini_path);
    }
    
    if (g_mod.playback.IsRunning()) {
        g_mod.playback.Stop();
        g_mod.playback.Start(g_mod.cfg.playback_device);
        g_mod.playback.SetMasterVolume(g_mod.cfg.master_volume);
    }
}

void GetCaptureDevice(char* out, size_t cap) {
    if (!out || cap == 0) return;
    std::strncpy(out, g_mod.cfg.capture_device, cap - 1);
    out[cap - 1] = 0;
}

void GetPlaybackDevice(char* out, size_t cap) {
    if (!out || cap == 0) return;
    std::strncpy(out, g_mod.cfg.playback_device, cap - 1);
    out[cap - 1] = 0;
}

void SetAuthToken(const char* /*token*/, uint32_t /*player_id*/) {
    // Deprecated — identity is resolved server-side via TCP source-
    // port matching now. Left as a no-op so the public API doesn't
    // break callers that wired it up (none today).
}

bool IsPlayerMuted(uint32_t player_id) {
    std::lock_guard<std::mutex> lk(g_local_prefs_mu);
    auto it = g_local_muted.find(player_id);
    return it != g_local_muted.end() ? it->second : false;
}

void SetPlayerMuted(uint32_t player_id, bool muted) {
    {
        std::lock_guard<std::mutex> lk(g_local_prefs_mu);
        g_local_muted[player_id] = muted;
    }
    UpdateActiveSpeakerLocalMuteVolume(player_id);
}

float GetPlayerVolume(uint32_t player_id) {
    std::lock_guard<std::mutex> lk(g_local_prefs_mu);
    auto it = g_local_volume.find(player_id);
    return it != g_local_volume.end() ? it->second : 1.0f;
}

void SetPlayerVolume(uint32_t player_id, float volume) {
    {
        std::lock_guard<std::mutex> lk(g_local_prefs_mu);
        g_local_volume[player_id] = volume;
    }
    UpdateActiveSpeakerLocalMuteVolume(player_id);
}

void SetCharName(const char* name) {
    if (!name) return;
    std::strncpy(g_mod.cfg.char_name, name, sizeof(g_mod.cfg.char_name) - 1);
    g_mod.cfg.char_name[sizeof(g_mod.cfg.char_name) - 1] = 0;
    if (g_mod.ini_path[0] != 0) {
        wchar_t wName[64] = {};
        size_t dummy = 0;
        mbstowcs_s(&dummy, wName, name, _TRUNCATE);
        WritePrivateProfileStringW(L"voice", L"char_name", wName, g_mod.ini_path);
    }
    RefreshClientPorts();
}

bool IsPrioritizeClanLeader() { return g_prioritize_clan_leader.load(); }
void SetPrioritizeClanLeader(bool active) { g_prioritize_clan_leader.store(active); }
bool IsPrioritizePartyLeader() { return g_prioritize_party_leader.load(); }
void SetPrioritizePartyLeader(bool active) { g_prioritize_party_leader.store(active); }

bool HasActiveSpeakers() {
    return g_mod.playback.ActiveSpeakers() > 0;
}

static bool g_supportWindowOpen = false;
bool IsSupportWindowOpen() {
    return g_supportWindowOpen;
}
void SetSupportWindowOpen(bool open) {
    g_supportWindowOpen = open;
}
void GetVoiceServerHost(char* out, size_t cap) {
    if (!out || cap == 0) return;
    const char* p = std::strstr(g_mod.cfg.ws_url, "://");
    if (p) {
        p += 3;
        size_t len = 0;
        while (p[len] && p[len] != '/' && p[len] != ':') {
            len++;
        }
        if (len < cap) {
            std::strncpy(out, p, len);
            out[len] = 0;
            return;
        }
    }
    std::strncpy(out, "127.0.0.1", cap - 1);
    out[cap - 1] = 0;
}

int GetPlayerSpeakingChannelImpl(const char* name) {
    if (!name || !name[0]) return -1;
    std::lock_guard<std::mutex> lk(g_local_prefs_mu);
    
    // First try exact match (fastest path)
    auto it = g_speaker_last_time.find(name);
    if (it != g_speaker_last_time.end()) {
        int64_t last_time = it->second;
        // Extended window to 600ms to account for 100ms polling interval
        // in BottomBar.uc timer
        if (NowMillis() - last_time < 600) {
            auto chIt = g_speaker_channel.find(name);
            return (chIt != g_speaker_channel.end()) ? chIt->second : -1;
        }
    }
    
    // Case-insensitive fallback: names from UnrealScript (pawnInfo.Name) may
    // have different capitalization than names from the voice server network.
    for (auto& kv : g_speaker_last_time) {
        if (_stricmp(kv.first.c_str(), name) == 0) {
            if (NowMillis() - kv.second < 600) {
                auto chIt = g_speaker_channel.find(kv.first);
                return (chIt != g_speaker_channel.end()) ? chIt->second : -1;
            }
        }
    }
    
    return -1;
}

}  // namespace voice

extern "C" __declspec(dllexport) int GetPlayerSpeakingChannel(const char* name) {
    return voice::GetPlayerSpeakingChannelImpl(name);
}
