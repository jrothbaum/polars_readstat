//! Exact conversion between `f64` and the base-30 numbers of SPSS portable (.por) files.
//!
//! A .por number is `[-] digits [. digits] [(+|-) exponent] /`, all in base 30 (`0-9`, `A-T`),
//! where the exponent is a power of 30. SPSS writes each value rounded to the number of
//! significant base-30 digits given in the file's precision record (IBM SPSS 25 uses 11),
//! drops trailing zeros, and uses the exponent for large magnitudes (`G9Q+4`).
//!
//! Both directions here are exact. The writer rounds the double's exact value to [`PRECISION`]
//! significant digits; the reader takes the exact value of all the digits it is given and
//! rounds it to the nearest double once. Twelve digits are at least 30^11 > 2^54, so the
//! written value is within half an ulp of the original and every finite double reads back
//! bit-identical, at any magnitude, subnormals included.

/// Significant base-30 digits the writer emits, and declares in the precision record.
pub(crate) const PRECISION: u32 = 12;

const LOG2_30: f64 = 4.906_890_595_608_519;

pub(crate) fn digit_value(c: u8) -> Option<u32> {
    match c {
        b'0'..=b'9' => Some((c - b'0') as u32),
        b'A'..=b'T' => Some((c - b'A') as u32 + 10),
        _ => None,
    }
}

pub(crate) fn digit_char(d: u32) -> u8 {
    if d < 10 { b'0' + d as u8 } else { b'A' + (d - 10) as u8 }
}

// ─── Reading ──────────────────────────────────────────────────────────────────

/// Parse a number without its '/' terminator (missing values, `*.`, are handled by the caller).
pub(crate) fn parse(chars: &[u8]) -> Result<f64, String> {
    let mut i = 0;
    let negative = chars.first() == Some(&b'-');
    if negative || chars.first() == Some(&b'+') {
        i = 1;
    }

    // value = mantissa * 30^(exp - fraction digits)
    let mut mant = Mantissa::Small(0);
    let mut scale: i64 = 0;
    let mut seen_point = false;
    while i < chars.len() {
        let c = chars[i];
        if c == b'+' || c == b'-' {
            break;
        }
        i += 1;
        if c == b'.' && !seen_point {
            seen_point = true;
            continue;
        }
        let d = digit_value(c).ok_or_else(|| format!("invalid base-30 digit '{}'", c as char))?;
        mant.push(d);
        if seen_point {
            scale -= 1;
        }
    }

    if i < chars.len() {
        let exp_neg = chars[i] == b'-';
        let mut exp: i64 = 0;
        for &c in &chars[i + 1..] {
            let d = digit_value(c)
                .ok_or_else(|| format!("invalid base-30 exponent digit '{}'", c as char))?;
            exp = exp.saturating_mul(30).saturating_add(d as i64);
        }
        scale = scale.saturating_add(if exp_neg { -exp } else { exp });
    }

    let v = mant.scale_to_f64(scale);
    Ok(if negative { -v } else { v })
}

enum Mantissa {
    Small(u128),
    Big(Big),
}

impl Mantissa {
    fn push(&mut self, d: u32) {
        match self {
            // below 2^122, `x * 30 + 29` stays below 2^127
            Mantissa::Small(x) if *x < (1u128 << 122) => *x = *x * 30 + d as u128,
            Mantissa::Small(x) => {
                let mut b = Big::from_u128(*x);
                b.mul_small(30);
                b.add_small(d);
                *self = Mantissa::Big(b);
            }
            Mantissa::Big(b) => {
                b.mul_small(30);
                b.add_small(d);
            }
        }
    }

    /// `self * 30^q`, rounded to the nearest double (ties to even).
    fn scale_to_f64(self, q: i64) -> f64 {
        let bits = match &self {
            Mantissa::Small(0) => return 0.0,
            Mantissa::Small(x) => 128 - x.leading_zeros() as i64,
            Mantissa::Big(b) if b.is_zero() => return 0.0,
            Mantissa::Big(b) => b.bit_len() as i64,
        };
        // the value lies in [2^(bits - 1 + q log2 30), 2^(bits + q log2 30))
        let log2_low = (bits - 1) as f64 + q as f64 * LOG2_30;
        if log2_low > 1025.0 {
            return f64::INFINITY;
        }
        if log2_low < -1080.0 {
            return 0.0;
        }

        // Exact for both operands, so one rounding: covers integers and short numbers.
        if let Mantissa::Small(x) = self {
            if x < (1u128 << 53) && q.unsigned_abs() <= 13 {
                let p = POW30_EXACT[q.unsigned_abs() as usize];
                return if q >= 0 { x as f64 * p } else { x as f64 / p };
            }
        }

        let mut big = match self {
            Mantissa::Small(x) => Big::from_u128(x),
            Mantissa::Big(b) => b,
        };
        if q >= 0 {
            big.mul_pow(30, q as u64);
            big.round_to_f64(0, false)
        } else {
            // Keep at least 66 bits in the quotient, so the rounding bits are all there.
            let n = q.unsigned_abs();
            let shift = (66 + (n as f64 * LOG2_30).ceil() as i64 - bits).max(0);
            big.shl(shift as u64);
            let sticky = big.div_pow30(n);
            big.round_to_f64(-shift, sticky)
        }
    }
}

/// 30^k for k <= 13: exact doubles (30^13 = 15^13 * 2^13, and 15^13 < 2^53).
const POW30_EXACT: [f64; 14] = {
    let mut t = [1.0f64; 14];
    let mut k = 1;
    while k < 14 {
        t[k] = t[k - 1] * 30.0;
        k += 1;
    }
    t
};

// ─── Writing ──────────────────────────────────────────────────────────────────

/// `abs` (finite, above zero) rounded to [`PRECISION`] significant base-30 digits (ties to
/// even), as `(m, q)` with value `m * 30^q` and no trailing zero digits in `m`.
pub(crate) fn decompose(abs: f64) -> (u64, i64) {
    debug_assert!(abs.is_finite() && abs > 0.0);
    let bits = abs.to_bits();
    let biased = ((bits >> 52) & 0x7FF) as i64;
    let fraction = bits & ((1u64 << 52) - 1);
    let (mut m2, mut e2) = if biased == 0 {
        (fraction, -1074i64)
    } else {
        (fraction | (1u64 << 52), biased - 1075)
    };
    let tz = m2.trailing_zeros();
    m2 >>= tz;
    e2 += tz as i64;

    // abs = n * 30^q exactly; for e2 < 0, 2^e2 = 15^-e2 / 30^-e2.
    let mut q: i64 = 0;
    let mut n: u128;
    if e2 >= 0 {
        if 64 - m2.leading_zeros() as i64 + e2 <= 127 {
            n = (m2 as u128) << e2;
        } else {
            let mut b = Big::from_u128(m2 as u128);
            b.shl(e2 as u64);
            (n, q) = b.reduce_to_u128();
        }
    } else {
        q = e2;
        let k = e2.unsigned_abs();
        let mut small = Some(m2 as u128);
        for _ in 0..k {
            small = small.and_then(|x| x.checked_mul(15));
        }
        match small.filter(|&x| x < STICKY_FLAG) {
            Some(x) => n = x,
            None => {
                let mut b = Big::from_u128(m2 as u128);
                b.mul_pow(15, k);
                let (x, dropped) = b.reduce_to_u128();
                n = x;
                q += dropped;
            }
        }
    }

    // Drop the digits beyond PRECISION in one division, rounding to nearest, ties to even.
    // Digits reduce_to_u128 already dropped count as below the halfway point's last digit.
    let sticky = n & STICKY_FLAG != 0;
    n &= !STICKY_FLAG;
    let mut drop = 0u32;
    // n < 2^127 < 30^26, so this stops by PRECISION + drop = 26
    while PRECISION + drop < 27 && n >= POW30_U128[(PRECISION + drop) as usize] {
        drop += 1;
    }
    let mut m = n as u64;
    if drop > 0 {
        let div = POW30_U128[drop as usize];
        let (quot, rem) = (n / div, n % div);
        m = quot as u64;
        let half = div / 2;
        if rem > half || (rem == half && (sticky || m % 2 == 1)) {
            m += 1;
        }
        q += drop as i64;
    }
    while m % 30 == 0 {
        m /= 30;
        q += 1;
    }
    (m, q)
}

/// 30^k for k <= 26 (30^26 < 2^128).
const POW30_U128: [u128; 27] = {
    let mut t = [1u128; 27];
    let mut k = 1;
    while k < 27 {
        t[k] = t[k - 1] * 30;
        k += 1;
    }
    t
};

/// The shortest `(m, q)` of at most [`PRECISION`] digits that reads back as exactly `abs`
/// (finite, above zero). Typical data is short in base 30: 1.1 is `1.3`, 0.01 is `0.09`.
pub(crate) fn shortest(abs: f64) -> (u64, i64) {
    let (m, q) = decompose(abs);
    let n = count_digits(m);
    // Binary search on the digit count: n digits always read back exactly. Candidates are
    // checked with the exact reader, so the result is always right; at worst it is a digit
    // longer than it could be.
    let reads_back = |(m, q): (u64, i64)| {
        Mantissa::Small(m as u128).scale_to_f64(q).to_bits() == abs.to_bits()
    };
    // Most computed values need every digit: settle those with a single check.
    if n <= 1 || !reads_back(round_to_digits(m, q, n, n - 1)) {
        return (m, q);
    }
    let (mut lo, mut hi) = (1u32, n - 1);
    let mut best = round_to_digits(m, q, n, n - 1);
    while lo < hi {
        let p = (lo + hi) / 2;
        let cand = round_to_digits(m, q, n, p);
        if reads_back(cand) {
            best = cand;
            hi = p;
        } else {
            lo = p + 1;
        }
    }
    best
}

fn count_digits(mut m: u64) -> u32 {
    let mut n = 0;
    while m > 0 {
        m /= 30;
        n += 1;
    }
    n
}

/// `m * 30^q` (m with n digits) rounded to p digits, ties to even, trailing zeros dropped.
fn round_to_digits(m: u64, q: i64, n: u32, p: u32) -> (u64, i64) {
    let div = 30u64.pow(n - p);
    let (mut r, rem) = (m / div, m % div);
    if rem > div / 2 || (rem == div / 2 && r % 2 == 1) {
        r += 1;
    }
    let mut q = q + (n - p) as i64;
    while r % 30 == 0 {
        r /= 30;
        q += 1;
    }
    (r, q)
}

/// Set in the result of `reduce_to_u128` when the digits it dropped were not all zero. The
/// values it returns are below 2^127, so the top bit is free.
const STICKY_FLAG: u128 = 1u128 << 127;

// ─── Minimal big unsigned integer ─────────────────────────────────────────────

/// Little-endian base-2^32 limbs, no leading zero limbs.
struct Big(Vec<u32>);

impl Big {
    fn from_u128(mut x: u128) -> Self {
        let mut v = Vec::with_capacity(8);
        while x != 0 {
            v.push(x as u32);
            x >>= 32;
        }
        Big(v)
    }

    fn is_zero(&self) -> bool {
        self.0.is_empty()
    }

    fn trim(&mut self) {
        while self.0.last() == Some(&0) {
            self.0.pop();
        }
    }

    fn bit_len(&self) -> u64 {
        match self.0.last() {
            None => 0,
            Some(&top) => self.0.len() as u64 * 32 - top.leading_zeros() as u64,
        }
    }

    fn mul_small(&mut self, m: u32) {
        let mut carry = 0u64;
        for limb in self.0.iter_mut() {
            let t = *limb as u64 * m as u64 + carry;
            *limb = t as u32;
            carry = t >> 32;
        }
        if carry != 0 {
            self.0.push(carry as u32);
        }
    }

    fn add_small(&mut self, a: u32) {
        let mut carry = a as u64;
        for limb in self.0.iter_mut() {
            if carry == 0 {
                return;
            }
            let t = *limb as u64 + carry;
            *limb = t as u32;
            carry = t >> 32;
        }
        if carry != 0 {
            self.0.push(carry as u32);
        }
    }

    /// `self *= base^n` for base 15 or 30, in the largest powers that fit a limb.
    fn mul_pow(&mut self, base: u32, mut n: u64) {
        let (chunk, chunk_pow) = if base == 15 { (8, 15u32.pow(8)) } else { (6, 30u32.pow(6)) };
        while n >= chunk {
            self.mul_small(chunk_pow);
            n -= chunk;
        }
        if n > 0 {
            self.mul_small(base.pow(n as u32));
        }
    }

    fn shl(&mut self, bits: u64) {
        if self.is_zero() || bits == 0 {
            return;
        }
        let limbs = (bits / 32) as usize;
        let r = (bits % 32) as u32;
        if r != 0 {
            let mut carry = 0u32;
            for limb in self.0.iter_mut() {
                let next = *limb >> (32 - r);
                *limb = (*limb << r) | carry;
                carry = next;
            }
            if carry != 0 {
                self.0.push(carry);
            }
        }
        if limbs > 0 {
            self.0.splice(0..0, std::iter::repeat(0).take(limbs));
        }
    }

    /// `self /= d`, returning the remainder.
    fn div_small(&mut self, d: u32) -> u32 {
        let mut rem = 0u64;
        for limb in self.0.iter_mut().rev() {
            let cur = (rem << 32) | *limb as u64;
            *limb = (cur / d as u64) as u32;
            rem = cur % d as u64;
        }
        self.trim();
        rem as u32
    }

    /// `self = floor(self / 30^n)`; true if the division was inexact. Floors compose:
    /// floor(floor(x / a) / b) = floor(x / ab).
    fn div_pow30(&mut self, mut n: u64) -> bool {
        let mut inexact = false;
        while n >= 6 {
            inexact |= self.div_small(30u32.pow(6)) != 0;
            n -= 6;
        }
        if n > 0 {
            inexact |= self.div_small(30u32.pow(n as u32)) != 0;
        }
        inexact
    }

    /// Divide by powers of 30 until the value fits below 2^127, keeping more than
    /// PRECISION + 1 digits. Returns the value (with STICKY_FLAG set if a dropped digit was
    /// nonzero) and how many digits were dropped.
    fn reduce_to_u128(mut self) -> (u128, i64) {
        let mut dropped = 0i64;
        let mut inexact = false;
        // At 2^127 or more there are at least 26 digits: dropping 6 leaves 20 > PRECISION + 1.
        while self.bit_len() > 127 {
            inexact |= self.div_small(30u32.pow(6)) != 0;
            dropped += 6;
        }
        let mut x = 0u128;
        for &limb in self.0.iter().rev() {
            x = (x << 32) | limb as u128;
        }
        (if inexact { x | STICKY_FLAG } else { x }, dropped)
    }

    fn bit(&self, i: u64) -> bool {
        let limb = (i / 32) as usize;
        limb < self.0.len() && (self.0[limb] >> (i % 32)) & 1 == 1
    }

    fn any_bits_below(&self, i: u64) -> bool {
        let limb = (i / 32) as usize;
        if self.0[..limb.min(self.0.len())].iter().any(|&l| l != 0) {
            return true;
        }
        let r = i % 32;
        limb < self.0.len() && r > 0 && self.0[limb] & ((1u32 << r) - 1) != 0
    }

    /// Bits `[from, from + 64)` as a u64.
    fn bits_from(&self, from: u64) -> u64 {
        let limb = (from / 32) as usize;
        let r = from % 32;
        let get = |i: usize| self.0.get(i).copied().unwrap_or(0) as u128;
        let window = get(limb) | get(limb + 1) << 32 | get(limb + 2) << 64;
        (window >> r) as u64
    }

    /// `self * 2^exp2`, plus a nonzero amount below the lowest bit if `sticky`, rounded to the
    /// nearest double (ties to even).
    fn round_to_f64(&self, exp2: i64, sticky: bool) -> f64 {
        let len = self.bit_len() as i64;
        if len == 0 {
            return 0.0;
        }
        let mut lsb = (len + exp2 - 53).max(-1074);
        let shift = lsb - exp2;
        let mut mant;
        if shift <= 0 {
            // fits in 53 bits as it is
            mant = self.bits_from(0) << (-shift);
        } else {
            let shift = shift as u64;
            mant = self.bits_from(shift);
            let half = self.bit(shift - 1);
            let rest = sticky || self.any_bits_below(shift - 1);
            if half && (rest || mant & 1 == 1) {
                mant += 1;
                if mant == 1 << 53 {
                    mant >>= 1;
                    lsb += 1;
                }
            }
        }
        if lsb > 1023 - 52 {
            return f64::INFINITY;
        }
        // both factors exact and the product representable: no further rounding
        mant as f64 * pow2(lsb)
    }
}

/// 2^e for -1074 <= e <= 1023, built from its bits.
fn pow2(e: i64) -> f64 {
    if e >= -1022 {
        f64::from_bits(((e + 1023) as u64) << 52)
    } else {
        f64::from_bits(1u64 << (e + 1074))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(v: f64) -> String {
        let (m, q) = shortest(v);
        let mut digits = Vec::new();
        let mut x = m;
        while x > 0 {
            digits.push(digit_char((x % 30) as u32));
            x /= 30;
        }
        digits.reverse();
        let mut s = String::from_utf8(digits).unwrap();
        if q != 0 {
            s.push(if q > 0 { '+' } else { '-' });
            let mut e = q.unsigned_abs();
            let mut ed = Vec::new();
            while e > 0 {
                ed.push(digit_char((e % 30) as u32));
                e /= 30;
            }
            ed.reverse();
            s.push_str(std::str::from_utf8(&ed).unwrap());
        }
        s
    }

    #[test]
    fn parses_spss_written_values() {
        // from tests/spss/data/sample.por, written by IBM SPSS 25
        assert_eq!(parse(b"1.3").unwrap(), 1.1);
        assert_eq!(parse(b"1.6").unwrap(), 1.2);
        assert_eq!(parse(b"-13A.9").unwrap(), -1000.3);
        assert_eq!(parse(b"G9Q+4").unwrap(), 11_903_760_000.0);
        assert_eq!(parse(b"0.F").unwrap(), 0.5);
        assert_eq!(parse(b".F").unwrap(), 0.5);
        assert_eq!(parse(b"F-1").unwrap(), 0.5);
        assert_eq!(parse(b"0").unwrap(), 0.0);
        assert_eq!(parse(b"1+TT").unwrap(), f64::INFINITY);
        assert_eq!(parse(b"1-TT").unwrap(), 0.0);
        assert!(parse(b"1.2.3").is_err());
        assert!(parse(b"1U").is_err());
    }

    #[test]
    fn writes_significant_digits() {
        assert_eq!(write(1.1), "13-1");
        assert_eq!(write(0.01), "9-2");
        assert_eq!(write(23.45), "NDF-2");
        assert_eq!(write(0.5), "F-1");
        assert_eq!(write(900.0), "1+2");
        assert_eq!(write(11_903_760_000.0), "G9Q+4");
    }

    #[test]
    fn long_fractions_round_correctly() {
        // 1/29 = 0.111... in base 30; 60 digits of it must still give the nearest double
        let s = format!("0.{}", "1".repeat(60));
        assert_eq!(parse(s.as_bytes()).unwrap(), 1.0 / 29.0);
        // and the 50-digit fractions of the old writer, with an exponent on top
        let s = format!("0.{}+2", "1".repeat(50));
        assert!((parse(s.as_bytes()).unwrap() - 900.0 / 29.0).abs() < 1e-12);
    }

    fn check_roundtrip(v: f64) {
        let s = write(v);
        let (m, _) = decompose(v);
        assert!(m < 30u64.pow(PRECISION));
        let got = parse(s.as_bytes()).unwrap();
        assert_eq!(got.to_bits(), v.to_bits(), "{v:e} -> {s} -> {got:e}");
    }

    #[test]
    fn every_double_roundtrips() {
        for v in [
            f64::from_bits(1),
            f64::from_bits((1 << 52) - 1),
            f64::MIN_POSITIVE,
            f64::MAX,
            f64::EPSILON,
            0.1,
            1.0 / 3.0,
            2.5e-300,
            1e-310,
            1e300,
            123456789.123456789,
            (1u64 << 53) as f64 - 1.0,
            1e17,
        ] {
            check_roundtrip(v);
        }
        let mut s: u64 = 0x9E37_79B9_7F4A_7C15;
        for _ in 0..300_000 {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            let v = f64::from_bits(s & !(1 << 63));
            if v.is_finite() && v != 0.0 {
                check_roundtrip(v);
            }
        }
    }
}
