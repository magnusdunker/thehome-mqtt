//! Small color conversions for lights that only accept one color format.

pub fn hs_to_rgb(h: f64, s: f64) -> (u8, u8, u8) {
    let s = (s / 100.0).clamp(0.0, 1.0);
    let h = (h.rem_euclid(360.0)) / 60.0;
    let c = s;
    let x = c * (1.0 - ((h % 2.0) - 1.0).abs());
    let (r, g, b) = match h as u32 {
        0 => (c, x, 0.0),
        1 => (x, c, 0.0),
        2 => (0.0, c, x),
        3 => (0.0, x, c),
        4 => (x, 0.0, c),
        _ => (c, 0.0, x),
    };
    let m = 1.0 - c;
    let to = |v: f64| ((v + m) * 255.0).round().clamp(0.0, 255.0) as u8;
    (to(r), to(g), to(b))
}

pub fn rgb_to_hs(r: f64, g: f64, b: f64) -> (f64, f64) {
    let (r, g, b) = (r / 255.0, g / 255.0, b / 255.0);
    let max = r.max(g).max(b);
    let min = r.min(g).min(b);
    let d = max - min;
    let h = if d == 0.0 {
        0.0
    } else if max == r {
        60.0 * (((g - b) / d).rem_euclid(6.0))
    } else if max == g {
        60.0 * ((b - r) / d + 2.0)
    } else {
        60.0 * ((r - g) / d + 4.0)
    };
    let s = if max == 0.0 { 0.0 } else { d / max * 100.0 };
    (round3(h), round3(s))
}

pub fn rgb_to_xy(r: f64, g: f64, b: f64) -> (f64, f64) {
    let lin = |c: f64| {
        let c = c / 255.0;
        if c > 0.04045 {
            ((c + 0.055) / 1.055).powf(2.4)
        } else {
            c / 12.92
        }
    };
    let (r, g, b) = (lin(r), lin(g), lin(b));
    let x = r * 0.664_511 + g * 0.154_324 + b * 0.162_028;
    let y = r * 0.283_881 + g * 0.668_433 + b * 0.047_685;
    let z = r * 0.000_088 + g * 0.072_310 + b * 0.986_039;
    let sum = x + y + z;
    if sum == 0.0 {
        return (0.0, 0.0);
    }
    (round3(x / sum), round3(y / sum))
}

pub fn xy_to_rgb(x: f64, y: f64) -> (u8, u8, u8) {
    if y == 0.0 {
        return (0, 0, 0);
    }
    let z = 1.0 - x - y;
    let yy = 1.0;
    let xx = yy / y * x;
    let zz = yy / y * z;
    let r = xx * 1.656_492 - yy * 0.354_851 - zz * 0.255_038;
    let g = -xx * 0.707_196 + yy * 1.655_397 + zz * 0.036_152;
    let b = xx * 0.051_713 - yy * 0.121_364 + zz * 1.011_530;
    let gamma = |c: f64| {
        if c <= 0.003_130_8 {
            12.92 * c
        } else {
            1.055 * c.powf(1.0 / 2.4) - 0.055
        }
    };
    let (r, g, b) = (gamma(r).max(0.0), gamma(g).max(0.0), gamma(b).max(0.0));
    let max = r.max(g).max(b).max(1e-9);
    let (r, g, b) = if max > 1.0 { (r / max, g / max, b / max) } else { (r, g, b) };
    let to = |v: f64| (v * 255.0).round().clamp(0.0, 255.0) as u8;
    (to(r), to(g), to(b))
}

pub fn kelvin_to_mired(k: f64) -> f64 {
    if k <= 0.0 {
        return 0.0;
    }
    (1_000_000.0 / k).round()
}

pub fn mired_to_kelvin(m: f64) -> f64 {
    if m <= 0.0 {
        return 0.0;
    }
    (1_000_000.0 / m).round()
}

fn round3(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips() {
        assert_eq!(hs_to_rgb(0.0, 100.0), (255, 0, 0));
        assert_eq!(hs_to_rgb(120.0, 100.0), (0, 255, 0));
        let (h, s) = rgb_to_hs(0.0, 0.0, 255.0);
        assert_eq!((h, s), (240.0, 100.0));
        assert_eq!(kelvin_to_mired(4000.0), 250.0);
        assert_eq!(mired_to_kelvin(250.0), 4000.0);
        let (x, y) = rgb_to_xy(255.0, 0.0, 0.0);
        assert!(x > 0.6 && y < 0.35);
        let (r, g, b) = xy_to_rgb(x, y);
        assert!(r > 200 && g < 60 && b < 60);
    }
}
