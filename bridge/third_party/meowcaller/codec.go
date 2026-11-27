package meowcaller

import "github.com/purpshell/meowcaller/signaling"

// selectAudioCodec maps parsed voip_settings to the codec the media loop should
// use. Absent settings or an explicit MLow selection keep the call on MLow; only
// use_mlow_codec_v1=false switches it to Opus.
func selectAudioCodec(vs *signaling.VoipSettings) AudioCodec {
	if vs == nil || !vs.Present || vs.UseMlowCodecV1 {
		return AudioCodecMlow
	}
	return AudioCodecOpus
}
