//! 16 kHz MLow float32 <-> 48 kHz PCM audio for WebRTC microphone capture.
//! 60 ms WhatsApp packets are split into six 10 ms WebRTC frames.
pub const RTC_FRAME: usize = 480;
#[cfg(test)]
pub const WA_FRAME: usize = 960;

pub fn upsample_16_to_48(src: &[f32], preceding: f32) -> Vec<i16> {
    let mut dst = Vec::with_capacity(src.len() * 3);
    for (i, &sample) in src.iter().enumerate() {
        let a = if i == 0 { preceding } else { src[i - 1] };
        for j in 0..3 {
            let x = a + (sample - a) * (j as f32 + 1.0) / 3.0;
            dst.push((x.clamp(-1.0, 1.0) * 32767.0).round() as i16);
        }
    }
    dst
}

pub fn downsample_48_to_16(src: &[i16]) -> Vec<f32> {
    src.chunks_exact(3).map(|s| {
        // Simple three-tap average; higher-quality FIR resampler remains needed.
        (f32::from(s[0]) + f32::from(s[1]) + f32::from(s[2])) / (3.0 * 32768.0)
    }).collect()
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn wav_round_trip() {
        let input: Vec<f32> = (0..WA_FRAME).map(|i| (i as f32 * 0.04).sin() * 0.3).collect();
        let up = upsample_16_to_48(&input, input[0]);
        assert_eq!(up.len(), 2880);
        assert_eq!(up.chunks_exact(RTC_FRAME).count(), 6);
        let down = downsample_48_to_16(&up);
        assert_eq!(down.len(), WA_FRAME);
        let error: f32 = input.iter().zip(&down).map(|(a,b)| (a - b).abs()).sum::<f32>() / WA_FRAME as f32;
        assert!(error < 0.005, "error {error}");
    }
}
