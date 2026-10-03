//! PostgreSQL's English Snowball stemmer: the generated `english_stem.c`
//! (Snowball 2.2.0's UTF-8 stemmer) transcribed with the subset of the
//! Snowball runtime it uses. The runtime mirrors `libstemmer/utilities.c`,
//! whose character helpers the stemmer's `_U` calls rely on.

/// One entry of an `among` table.
struct Among {
    s: &'static [u8],
    substring_i: i32,
    result: i32,
}

/// The stemmer's environment: the string, the cursor, and the three integer
/// variables (`p1`, `p2`, and `Y_found`).
struct Sn {
    p: Vec<u8>,
    c: usize,
    l: usize,
    lb: usize,
    bra: usize,
    ket: usize,
    i: [usize; 3],
}

// ------------------------------------------------------------- runtime

/// `get_utf8`: the character at `p[c]` and its width.
fn get_utf8(p: &[u8], c: usize, l: usize) -> Option<(u32, usize)> {
    if c >= l {
        return None;
    }
    let b0 = p[c] as u32;
    let mut c = c + 1;
    if b0 < 0xC0 || c == l {
        return Some((b0, 1));
    }
    let b1 = (p[c] as u32 & 0x3F) as u32;
    c += 1;
    if b0 < 0xE0 || c == l {
        return Some(((b0 & 0x1F) << 6 | b1, 2));
    }
    let b2 = (p[c] as u32 & 0x3F) as u32;
    c += 1;
    if b0 < 0xF0 || c == l {
        return Some(((b0 & 0xF) << 12 | b1 << 6 | b2, 3));
    }
    Some((
        (b0 & 0x7) << 18 | b1 << 12 | b2 << 6 | (p[c] as u32 & 0x3F),
        4,
    ))
}

/// `get_b_utf8`: the character ending before `p[c]` and its width.
fn get_b_utf8(p: &[u8], c: usize, lb: usize) -> Option<(u32, usize)> {
    if c <= lb {
        return None;
    }
    let mut c = c - 1;
    let mut b = p[c] as u32;
    if b < 0x80 || c == lb {
        return Some((b, 1));
    }
    let mut a = b & 0x3F;
    c -= 1;
    b = p[c] as u32;
    if b >= 0xC0 || c == lb {
        return Some(((b & 0x1F) << 6 | a, 2));
    }
    a |= (b & 0x3F) << 6;
    c -= 1;
    b = p[c] as u32;
    if b >= 0xE0 || c == lb {
        return Some(((b & 0xF) << 12 | a, 3));
    }
    c -= 1;
    Some(((p[c] as u32 & 0x7) << 18 | (b & 0x3F) << 12 | a, 4))
}

/// `skip_utf8`: `n` characters forward from `c`, or `None` at the limit.
fn skip_utf8(p: &[u8], c: usize, limit: usize, n: usize) -> Option<usize> {
    let mut c = c;
    for _ in 0..n {
        if c >= limit {
            return None;
        }
        let b = p[c];
        c += 1;
        if b >= 0xC0 {
            while c < limit {
                let b = p[c];
                if b >= 0xC0 || b < 0x80 {
                    break;
                }
                c += 1;
            }
        }
    }
    Some(c)
}

/// `skip_b_utf8`: `n` characters backward from `c`, or `None` at the limit.
fn skip_b_utf8(p: &[u8], c: usize, limit: usize, n: usize) -> Option<usize> {
    let mut c = c;
    for _ in 0..n {
        if c <= limit {
            return None;
        }
        c -= 1;
        if p[c] >= 0x80 {
            while c > limit {
                if p[c] >= 0xC0 {
                    break;
                }
                c -= 1;
            }
        }
    }
    Some(c)
}

fn in_grouping_bitmap(g: &[u8], ch: i64, min: i64, max: i64) -> bool {
    ch <= max && {
        let ch = ch - min;
        ch >= 0 && (g[(ch >> 3) as usize] as i64 & (1 << (ch & 7))) != 0
    }
}

/// `in_grouping_U`: steps over one in-group character; `0` on success, the
/// character's width when the test fails, `-1` at the end of the string.
fn in_grouping_u(z: &mut Sn, g: &[u8], min: i64, max: i64, repeat: bool) -> i32 {
    loop {
        let Some((ch, w)) = get_utf8(&z.p, z.c, z.l) else {
            return -1;
        };
        if !in_grouping_bitmap(g, ch as i64, min, max) {
            return w as i32;
        }
        z.c += w;
        if !repeat {
            return 0;
        }
    }
}

/// `in_grouping_b_U`.
fn in_grouping_b_u(z: &mut Sn, g: &[u8], min: i64, max: i64, repeat: bool) -> i32 {
    loop {
        let Some((ch, w)) = get_b_utf8(&z.p, z.c, z.lb) else {
            return -1;
        };
        if !in_grouping_bitmap(g, ch as i64, min, max) {
            return w as i32;
        }
        z.c -= w;
        if !repeat {
            return 0;
        }
    }
}

/// `out_grouping_U`: steps over one out-of-group character.
fn out_grouping_u(z: &mut Sn, g: &[u8], min: i64, max: i64, repeat: bool) -> i32 {
    loop {
        let Some((ch, w)) = get_utf8(&z.p, z.c, z.l) else {
            return -1;
        };
        if in_grouping_bitmap(g, ch as i64, min, max) {
            return w as i32;
        }
        z.c += w;
        if !repeat {
            return 0;
        }
    }
}

/// `out_grouping_b_U`.
fn out_grouping_b_u(z: &mut Sn, g: &[u8], min: i64, max: i64, repeat: bool) -> i32 {
    loop {
        let Some((ch, w)) = get_b_utf8(&z.p, z.c, z.lb) else {
            return -1;
        };
        if in_grouping_bitmap(g, ch as i64, min, max) {
            return w as i32;
        }
        z.c -= w;
        if !repeat {
            return 0;
        }
    }
}

/// `eq_s`: the literal string at the cursor.
fn eq_s(z: &mut Sn, s: &[u8]) -> bool {
    if z.l - z.c < s.len() || z.p[z.c..z.c + s.len()] != *s {
        return false;
    }
    z.c += s.len();
    true
}

/// `eq_s_b`: the literal string ending at the cursor.
fn eq_s_b(z: &mut Sn, s: &[u8]) -> bool {
    if z.c - z.lb < s.len() || z.p[z.c - s.len()..z.c] != *s {
        return false;
    }
    z.c -= s.len();
    true
}

/// `find_among`.
fn find_among(z: &mut Sn, v: &'static [Among]) -> i32 {
    let mut i = 0usize;
    let mut j = v.len();
    let c = z.c;
    let l = z.l;
    let mut common_i = 0usize;
    let mut common_j = 0usize;
    let mut first_key_inspected = false;
    loop {
        let k = i + ((j - i) >> 1);
        let mut diff = 0i32;
        let mut common = common_i.min(common_j);
        let w = &v[k];
        let mut i2 = common;
        while i2 < w.s.len() {
            if c + common == l {
                diff = -1;
                break;
            }
            diff = z.p[c + common] as i32 - w.s[i2] as i32;
            if diff != 0 {
                break;
            }
            common += 1;
            i2 += 1;
        }
        if diff < 0 {
            j = k;
            common_j = common;
        } else {
            i = k;
            common_i = common;
        }
        if j - i <= 1 {
            if i > 0 {
                break;
            }
            if j == i {
                break;
            }
            if first_key_inspected {
                break;
            }
            first_key_inspected = true;
        }
    }
    loop {
        let w = &v[i];
        if common_i >= w.s.len() {
            z.c = c + w.s.len();
            return w.result;
        }
        if w.substring_i < 0 {
            return 0;
        }
        i = w.substring_i as usize;
    }
}

/// `find_among_b`.
fn find_among_b(z: &mut Sn, v: &'static [Among]) -> i32 {
    let mut i = 0usize;
    let mut j = v.len();
    let c = z.c;
    let lb = z.lb;
    let mut common_i = 0usize;
    let mut common_j = 0usize;
    let mut first_key_inspected = false;
    loop {
        let k = i + ((j - i) >> 1);
        let mut diff = 0i32;
        let mut common = common_i.min(common_j);
        let w = &v[k];
        let mut i2 = w.s.len() as i64 - 1 - common as i64;
        while i2 >= 0 {
            if c - common == lb {
                diff = -1;
                break;
            }
            diff = z.p[c - common - 1] as i32 - w.s[i2 as usize] as i32;
            if diff != 0 {
                break;
            }
            common += 1;
            i2 -= 1;
        }
        if diff < 0 {
            j = k;
            common_j = common;
        } else {
            i = k;
            common_i = common;
        }
        if j - i <= 1 {
            if i > 0 {
                break;
            }
            if j == i {
                break;
            }
            if first_key_inspected {
                break;
            }
            first_key_inspected = true;
        }
    }
    loop {
        let w = &v[i];
        if common_i >= w.s.len() {
            z.c = c - w.s.len();
            return w.result;
        }
        if w.substring_i < 0 {
            return 0;
        }
        i = w.substring_i as usize;
    }
}

/// `replace_s`: replaces `[bra, ket)` with `s`, adjusting the cursor and the
/// limit as the runtime does.
fn replace_s(z: &mut Sn, bra: usize, ket: usize, s: &[u8]) {
    let adjustment = s.len() as isize - (ket - bra) as isize;
    if adjustment != 0 {
        z.p.splice(bra..ket, s.iter().copied()).for_each(drop);
        z.l = (z.l as isize + adjustment) as usize;
        if z.c >= ket {
            z.c = (z.c as isize + adjustment) as usize;
        } else if z.c > bra {
            z.c = bra;
        }
    } else if !s.is_empty() {
        z.p[bra..ket].copy_from_slice(s);
    }
}

/// `slice_from_s`.
fn slice_from_s(z: &mut Sn, s: &[u8]) {
    let (bra, ket) = (z.bra, z.ket);
    replace_s(z, bra, ket, s);
}

/// `slice_del`.
fn slice_del(z: &mut Sn) {
    slice_from_s(z, &[]);
}

// ------------------------------------------------------------ routines

/// `r_prelude`: drops a leading apostrophe and marks every `y` that follows
/// a vowel as a consonant (`Y`).
fn r_prelude(z: &mut Sn) {
    z.i[2] = 0;

    {
        let c1 = z.c;
        z.bra = z.c;
        if z.c != z.l && z.p[z.c] == b'\'' {
            z.c += 1;
            z.ket = z.c;
            slice_del(z);
        }
        z.c = c1;
    }

    {
        let c2 = z.c;
        z.bra = z.c;
        if z.c != z.l && z.p[z.c] == b'y' {
            z.c += 1;
            z.ket = z.c;
            slice_from_s(z, S_0);
            z.i[2] = 1;
        }
        z.c = c2;
    }

    {
        let c3 = z.c;
        'outer: loop {
            let c4 = z.c;
            loop {
                let c5 = z.c;
                if in_grouping_u(z, G_V, 97, 121, false) == 0 {
                    z.bra = z.c;
                    if z.c != z.l && z.p[z.c] == b'y' {
                        z.c += 1;
                        z.ket = z.c;
                        z.c = c5;
                        break;
                    }
                }
                z.c = c5;
                match skip_utf8(&z.p, z.c, z.l, 1) {
                    Some(next) => z.c = next,
                    None => {
                        z.c = c4;
                        break 'outer;
                    }
                }
            }
            slice_from_s(z, S_1);
            z.i[2] = 1;
        }
        z.c = c3;
    }
}

/// `r_mark_regions`: the region after the first non-vowel following a vowel
/// (`p1`), and the same after that (`p2`).
fn r_mark_regions(z: &mut Sn) {
    z.i[1] = z.l;
    z.i[0] = z.l;
    let c1 = z.c;
    'lab0: {
        {
            let c2 = z.c;
            if z.c + 4 < z.l
                && z.p[z.c + 4] >> 5 == 3
                && ((2375680 >> (z.p[z.c + 4] & 0x1f)) & 1) != 0
                && find_among(z, A_0) != 0
            {
                // A special prefix.
            } else {
                z.c = c2;
                let ret = out_grouping_u(z, G_V, 97, 121, true);
                if ret < 0 {
                    break 'lab0;
                }
                z.c += ret as usize;
                let ret = in_grouping_u(z, G_V, 97, 121, true);
                if ret < 0 {
                    break 'lab0;
                }
                z.c += ret as usize;
            }
        }
        z.i[1] = z.c;

        let ret = out_grouping_u(z, G_V, 97, 121, true);
        if ret < 0 {
            break 'lab0;
        }
        z.c += ret as usize;
        let ret = in_grouping_u(z, G_V, 97, 121, true);
        if ret < 0 {
            break 'lab0;
        }
        z.c += ret as usize;
        z.i[0] = z.c;
    }
    z.c = c1;
}

/// `r_shortv`: a short syllable ending at the cursor.
fn r_shortv(z: &mut Sn) -> bool {
    let m1 = z.l - z.c;
    {
        if out_grouping_b_u(z, G_V_WXY, 89, 121, false) != 0 {
            z.c = z.l - m1;
        } else if in_grouping_b_u(z, G_V, 97, 121, false) != 0 {
            z.c = z.l - m1;
        } else if out_grouping_b_u(z, G_V, 97, 121, false) != 0 {
            z.c = z.l - m1;
        } else {
            return true;
        }
    }
    if out_grouping_b_u(z, G_V, 97, 121, false) != 0 {
        return false;
    }
    if in_grouping_b_u(z, G_V, 97, 121, false) != 0 {
        return false;
    }
    if z.c > z.lb {
        return false;
    }
    true
}

/// `r_R1`: the cursor is past `p1`.
fn r_R1(z: &Sn) -> bool {
    z.i[1] <= z.c
}

/// `r_R2`: the cursor is past `p2`.
fn r_R2(z: &Sn) -> bool {
    z.i[0] <= z.c
}

/// `r_Step_1a`: plural and possessive endings.
fn r_Step_1a(z: &mut Sn) -> bool {
    {
        let m1 = z.l - z.c;
        z.ket = z.c;
        if z.c > z.lb && (z.p[z.c - 1] == 39 || z.p[z.c - 1] == 115) {
            if find_among_b(z, A_1) != 0 {
                z.bra = z.c;
                slice_del(z);
            } else {
                z.c = z.l - m1;
            }
        } else {
            z.c = z.l - m1;
        }
    }
    z.ket = z.c;
    if z.c <= z.lb || (z.p[z.c - 1] != 100 && z.p[z.c - 1] != 115) {
        return false;
    }
    let among_var = find_among_b(z, A_2);
    if among_var == 0 {
        return false;
    }
    z.bra = z.c;
    match among_var {
        1 => slice_from_s(z, S_2),
        2 => {
            let m2 = z.l - z.c;
            match skip_b_utf8(&z.p, z.c, z.lb, 2) {
                Some(next) => {
                    z.c = next;
                    slice_from_s(z, S_3);
                }
                None => {
                    z.c = z.l - m2;
                    slice_from_s(z, S_4);
                }
            }
        }
        3 => {
            match skip_b_utf8(&z.p, z.c, z.lb, 1) {
                Some(next) => z.c = next,
                None => return false,
            }
            let ret = out_grouping_b_u(z, G_V, 97, 121, true);
            if ret < 0 {
                return false;
            }
            z.c -= ret as usize;
            slice_del(z);
        }
        _ => {}
    }
    true
}

/// `r_Step_1b`: `eed`, `ed`, `edly`, `ing`, and `ingly`.
fn r_Step_1b(z: &mut Sn) -> bool {
    z.ket = z.c;
    if z.c < z.lb + 1 || z.p[z.c - 1] >> 5 != 3 || ((33554576 >> (z.p[z.c - 1] & 0x1f)) & 1) == 0 {
        return false;
    }
    let mut among_var = find_among_b(z, A_4);
    if among_var == 0 {
        return false;
    }
    z.bra = z.c;
    match among_var {
        1 => {
            if !r_R1(z) {
                return false;
            }
            slice_from_s(z, S_5);
        }
        2 => {
            let m_test1 = z.l - z.c;
            let ret = out_grouping_b_u(z, G_V, 97, 121, true);
            if ret < 0 {
                return false;
            }
            z.c -= ret as usize;
            z.c = z.l - m_test1;

            slice_del(z);
            z.ket = z.c;
            z.bra = z.c;
            let m_test2 = z.l - z.c;
            if z.c < z.lb + 1
                || z.p[z.c - 1] >> 5 != 3
                || ((68514004 >> (z.p[z.c - 1] & 0x1f)) & 1) == 0
            {
                among_var = 3;
            } else {
                among_var = find_among_b(z, A_3);
            }
            match among_var {
                1 => {
                    slice_from_s(z, S_6);
                    return false;
                }
                2 => {
                    let m3 = z.l - z.c;
                    let short = in_grouping_b_u(z, G_AEO, 97, 111, false) == 0 && z.c <= z.lb;
                    if !short {
                        z.c = z.l - m3;
                    } else {
                        return false;
                    }
                }
                3 => {
                    if z.c != z.i[1] {
                        return false;
                    }
                    let m_test4 = z.l - z.c;
                    if !r_shortv(z) {
                        return false;
                    }
                    z.c = z.l - m_test4;
                    slice_from_s(z, S_7);
                    return false;
                }
                _ => {}
            }
            z.c = z.l - m_test2;

            z.ket = z.c;
            match skip_b_utf8(&z.p, z.c, z.lb, 1) {
                Some(next) => z.c = next,
                None => return false,
            }
            z.bra = z.c;
            slice_del(z);
        }
        _ => {}
    }
    true
}

/// `r_Step_1c`: a `y` or `Y` after a non-vowel becomes `i`.
fn r_Step_1c(z: &mut Sn) -> bool {
    z.ket = z.c;
    {
        let m1 = z.l - z.c;
        if z.c > z.lb && z.p[z.c - 1] == b'y' {
            z.c -= 1;
        } else {
            z.c = z.l - m1;
            if z.c <= z.lb || z.p[z.c - 1] != b'Y' {
                return false;
            }
            z.c -= 1;
        }
    }
    z.bra = z.c;
    if out_grouping_b_u(z, G_V, 97, 121, false) != 0 {
        return false;
    }
    if z.c <= z.lb {
        return false;
    }
    slice_from_s(z, S_8);
    true
}

/// `r_Step_2`.
fn r_Step_2(z: &mut Sn) -> bool {
    z.ket = z.c;
    if z.c < z.lb + 1 || z.p[z.c - 1] >> 5 != 3 || ((815616 >> (z.p[z.c - 1] & 0x1f)) & 1) == 0 {
        return false;
    }
    let among_var = find_among_b(z, A_5);
    if among_var == 0 {
        return false;
    }
    z.bra = z.c;
    if !r_R1(z) {
        return false;
    }
    match among_var {
        1 => slice_from_s(z, S_9),
        2 => slice_from_s(z, S_10),
        3 => slice_from_s(z, S_11),
        4 => slice_from_s(z, S_12),
        5 => slice_from_s(z, S_13),
        6 => slice_from_s(z, S_14),
        7 => slice_from_s(z, S_15),
        8 => slice_from_s(z, S_16),
        9 => slice_from_s(z, S_17),
        10 => slice_from_s(z, S_18),
        11 => slice_from_s(z, S_19),
        12 => slice_from_s(z, S_20),
        13 => {
            if z.c <= z.lb || z.p[z.c - 1] != b'l' {
                return false;
            }
            z.c -= 1;
            slice_from_s(z, S_21);
        }
        14 => slice_from_s(z, S_22),
        15 => {
            if in_grouping_b_u(z, G_VALID_LI, 99, 116, false) != 0 {
                return false;
            }
            slice_del(z);
        }
        _ => {}
    }
    true
}

/// `r_Step_3`.
fn r_Step_3(z: &mut Sn) -> bool {
    z.ket = z.c;
    if z.c < z.lb + 2 || z.p[z.c - 1] >> 5 != 3 || ((528928 >> (z.p[z.c - 1] & 0x1f)) & 1) == 0 {
        return false;
    }
    let among_var = find_among_b(z, A_6);
    if among_var == 0 {
        return false;
    }
    z.bra = z.c;
    if !r_R1(z) {
        return false;
    }
    match among_var {
        1 => slice_from_s(z, S_23),
        2 => slice_from_s(z, S_24),
        3 => slice_from_s(z, S_25),
        4 => slice_from_s(z, S_26),
        5 => slice_del(z),
        6 => {
            if !r_R2(z) {
                return false;
            }
            slice_del(z);
        }
        _ => {}
    }
    true
}

/// `r_Step_4`.
fn r_Step_4(z: &mut Sn) -> bool {
    z.ket = z.c;
    if z.c < z.lb + 1 || z.p[z.c - 1] >> 5 != 3 || ((1864232 >> (z.p[z.c - 1] & 0x1f)) & 1) == 0 {
        return false;
    }
    let among_var = find_among_b(z, A_7);
    if among_var == 0 {
        return false;
    }
    z.bra = z.c;
    if !r_R2(z) {
        return false;
    }
    match among_var {
        1 => slice_del(z),
        2 => {
            let m1 = z.l - z.c;
            if z.c > z.lb && z.p[z.c - 1] == b's' {
                z.c -= 1;
            } else {
                z.c = z.l - m1;
                if z.c <= z.lb || z.p[z.c - 1] != b't' {
                    return false;
                }
                z.c -= 1;
            }
            slice_del(z);
        }
        _ => {}
    }
    true
}

/// `r_Step_5`.
fn r_Step_5(z: &mut Sn) -> bool {
    z.ket = z.c;
    if z.c <= z.lb || (z.p[z.c - 1] != 101 && z.p[z.c - 1] != 108) {
        return false;
    }
    let among_var = find_among_b(z, A_8);
    if among_var == 0 {
        return false;
    }
    z.bra = z.c;
    match among_var {
        1 => {
            if !r_R2(z) {
                if !r_R1(z) {
                    return false;
                }
                let m1 = z.l - z.c;
                if r_shortv(z) {
                    return false;
                }
                z.c = z.l - m1;
            }
            slice_del(z);
        }
        2 => {
            if !r_R2(z) {
                return false;
            }
            if z.c <= z.lb || z.p[z.c - 1] != b'l' {
                return false;
            }
            z.c -= 1;
            slice_del(z);
        }
        _ => {}
    }
    true
}

/// `r_exception2`: the words that keep `ed`/`ing` endings.
fn r_exception2(z: &mut Sn) -> bool {
    z.ket = z.c;
    if z.c < z.lb + 5 || (z.p[z.c - 1] != 100 && z.p[z.c - 1] != 103) {
        return false;
    }
    if find_among_b(z, A_9) == 0 {
        return false;
    }
    z.bra = z.c;
    if z.c > z.lb {
        return false;
    }
    true
}

/// `r_exception1`: the words that are left alone.
fn r_exception1(z: &mut Sn) -> bool {
    z.bra = z.c;
    if z.c + 2 >= z.l || z.p[z.c + 2] >> 5 != 3 || ((42750482 >> (z.p[z.c + 2] & 0x1f)) & 1) == 0 {
        return false;
    }
    let among_var = find_among(z, A_10);
    if among_var == 0 {
        return false;
    }
    z.ket = z.c;
    if z.c < z.l {
        return false;
    }
    match among_var {
        1 => slice_from_s(z, S_27),
        2 => slice_from_s(z, S_28),
        3 => slice_from_s(z, S_29),
        4 => slice_from_s(z, S_30),
        5 => slice_from_s(z, S_31),
        6 => slice_from_s(z, S_32),
        7 => slice_from_s(z, S_33),
        8 => slice_from_s(z, S_34),
        9 => slice_from_s(z, S_35),
        10 => slice_from_s(z, S_36),
        11 => slice_from_s(z, S_37),
        _ => {}
    }
    true
}

/// `r_postlude`: turns the marked `Y`s back into `y`.
fn r_postlude(z: &mut Sn) -> bool {
    if z.i[2] == 0 {
        return false;
    }
    loop {
        let c1 = z.c;
        loop {
            let c2 = z.c;
            z.bra = z.c;
            if z.c != z.l && z.p[z.c] == b'Y' {
                z.c += 1;
                z.ket = z.c;
                z.c = c2;
                break;
            }
            z.c = c2;
            match skip_utf8(&z.p, z.c, z.l, 1) {
                Some(next) => z.c = next,
                None => {
                    z.c = c1;
                    return true;
                }
            }
        }
        slice_from_s(z, S_38);
    }
}

/// `english_UTF_8_stem`.
pub(crate) fn stem(word: &str) -> String {
    let mut z = Sn {
        p: word.as_bytes().to_vec(),
        c: 0,
        l: word.len(),
        lb: 0,
        bra: 0,
        ket: 0,
        i: [0; 3],
    };

    let c1 = z.c;
    if !r_exception1(&mut z) {
        z.c = c1;
        let c2 = z.c;
        if skip_utf8(&z.p, z.c, z.l, 3).is_none() {
            z.c = c2;
            return String::from_utf8_lossy(&z.p).into_owned();
        }
        z.c = c1;

        r_prelude(&mut z);
        r_mark_regions(&mut z);
        z.lb = z.c;
        z.c = z.l;

        {
            let m3 = z.l - z.c;
            r_Step_1a(&mut z);
            z.c = z.l - m3;
        }
        {
            let m4 = z.l - z.c;
            if !r_exception2(&mut z) {
                z.c = z.l - m4;
                let m5 = z.l - z.c;
                r_Step_1b(&mut z);
                z.c = z.l - m5;
                let m6 = z.l - z.c;
                r_Step_1c(&mut z);
                z.c = z.l - m6;
                let m7 = z.l - z.c;
                r_Step_2(&mut z);
                z.c = z.l - m7;
                let m8 = z.l - z.c;
                r_Step_3(&mut z);
                z.c = z.l - m8;
                let m9 = z.l - z.c;
                r_Step_4(&mut z);
                z.c = z.l - m9;
                let m10 = z.l - z.c;
                r_Step_5(&mut z);
                z.c = z.l - m10;
            }
        }
        z.c = z.lb;
        let c11 = z.c;
        r_postlude(&mut z);
        z.c = c11;
    }
    String::from_utf8_lossy(&z.p).into_owned()
}

// ------------------------------------------------------- tables

const S_0: &[u8] = &[89];
const S_0_0: &[u8] = &[97, 114, 115, 101, 110];
const S_0_1: &[u8] = &[99, 111, 109, 109, 117, 110];
const S_0_2: &[u8] = &[103, 101, 110, 101, 114];
const S_1: &[u8] = &[89];
const S_10: &[u8] = &[101, 110, 99, 101];
const S_10_0: &[u8] = &[97, 110, 100, 101, 115];
const S_10_1: &[u8] = &[97, 116, 108, 97, 115];
const S_10_10: &[u8] = &[110, 101, 119, 115];
const S_10_11: &[u8] = &[111, 110, 108, 121];
const S_10_12: &[u8] = &[115, 105, 110, 103, 108, 121];
const S_10_13: &[u8] = &[115, 107, 105, 101, 115];
const S_10_14: &[u8] = &[115, 107, 105, 115];
const S_10_15: &[u8] = &[115, 107, 121];
const S_10_16: &[u8] = &[116, 121, 105, 110, 103];
const S_10_17: &[u8] = &[117, 103, 108, 121];
const S_10_2: &[u8] = &[98, 105, 97, 115];
const S_10_3: &[u8] = &[99, 111, 115, 109, 111, 115];
const S_10_4: &[u8] = &[100, 121, 105, 110, 103];
const S_10_5: &[u8] = &[101, 97, 114, 108, 121];
const S_10_6: &[u8] = &[103, 101, 110, 116, 108, 121];
const S_10_7: &[u8] = &[104, 111, 119, 101];
const S_10_8: &[u8] = &[105, 100, 108, 121];
const S_10_9: &[u8] = &[108, 121, 105, 110, 103];
const S_11: &[u8] = &[97, 110, 99, 101];
const S_12: &[u8] = &[97, 98, 108, 101];
const S_13: &[u8] = &[101, 110, 116];
const S_14: &[u8] = &[105, 122, 101];
const S_15: &[u8] = &[97, 116, 101];
const S_16: &[u8] = &[97, 108];
const S_17: &[u8] = &[102, 117, 108];
const S_18: &[u8] = &[111, 117, 115];
const S_19: &[u8] = &[105, 118, 101];
const S_1_0: &[u8] = &[39];
const S_1_1: &[u8] = &[39, 115, 39];
const S_1_2: &[u8] = &[39, 115];
const S_2: &[u8] = &[115, 115];
const S_20: &[u8] = &[98, 108, 101];
const S_21: &[u8] = &[111, 103];
const S_22: &[u8] = &[108, 101, 115, 115];
const S_23: &[u8] = &[116, 105, 111, 110];
const S_24: &[u8] = &[97, 116, 101];
const S_25: &[u8] = &[97, 108];
const S_26: &[u8] = &[105, 99];
const S_27: &[u8] = &[115, 107, 105];
const S_28: &[u8] = &[115, 107, 121];
const S_29: &[u8] = &[100, 105, 101];
const S_2_0: &[u8] = &[105, 101, 100];
const S_2_1: &[u8] = &[115];
const S_2_2: &[u8] = &[105, 101, 115];
const S_2_3: &[u8] = &[115, 115, 101, 115];
const S_2_4: &[u8] = &[115, 115];
const S_2_5: &[u8] = &[117, 115];
const S_3: &[u8] = &[105];
const S_30: &[u8] = &[108, 105, 101];
const S_31: &[u8] = &[116, 105, 101];
const S_32: &[u8] = &[105, 100, 108];
const S_33: &[u8] = &[103, 101, 110, 116, 108];
const S_34: &[u8] = &[117, 103, 108, 105];
const S_35: &[u8] = &[101, 97, 114, 108, 105];
const S_36: &[u8] = &[111, 110, 108, 105];
const S_37: &[u8] = &[115, 105, 110, 103, 108];
const S_38: &[u8] = &[121];
const S_3_1: &[u8] = &[98, 98];
const S_3_10: &[u8] = &[97, 116];
const S_3_11: &[u8] = &[116, 116];
const S_3_12: &[u8] = &[105, 122];
const S_3_2: &[u8] = &[100, 100];
const S_3_3: &[u8] = &[102, 102];
const S_3_4: &[u8] = &[103, 103];
const S_3_5: &[u8] = &[98, 108];
const S_3_6: &[u8] = &[109, 109];
const S_3_7: &[u8] = &[110, 110];
const S_3_8: &[u8] = &[112, 112];
const S_3_9: &[u8] = &[114, 114];
const S_4: &[u8] = &[105, 101];
const S_4_0: &[u8] = &[101, 100];
const S_4_1: &[u8] = &[101, 101, 100];
const S_4_2: &[u8] = &[105, 110, 103];
const S_4_3: &[u8] = &[101, 100, 108, 121];
const S_4_4: &[u8] = &[101, 101, 100, 108, 121];
const S_4_5: &[u8] = &[105, 110, 103, 108, 121];
const S_5: &[u8] = &[101, 101];
const S_5_0: &[u8] = &[97, 110, 99, 105];
const S_5_1: &[u8] = &[101, 110, 99, 105];
const S_5_10: &[u8] = &[101, 110, 116, 108, 105];
const S_5_11: &[u8] = &[97, 108, 105, 116, 105];
const S_5_12: &[u8] = &[98, 105, 108, 105, 116, 105];
const S_5_13: &[u8] = &[105, 118, 105, 116, 105];
const S_5_14: &[u8] = &[116, 105, 111, 110, 97, 108];
const S_5_15: &[u8] = &[97, 116, 105, 111, 110, 97, 108];
const S_5_16: &[u8] = &[97, 108, 105, 115, 109];
const S_5_17: &[u8] = &[97, 116, 105, 111, 110];
const S_5_18: &[u8] = &[105, 122, 97, 116, 105, 111, 110];
const S_5_19: &[u8] = &[105, 122, 101, 114];
const S_5_2: &[u8] = &[111, 103, 105];
const S_5_20: &[u8] = &[97, 116, 111, 114];
const S_5_21: &[u8] = &[105, 118, 101, 110, 101, 115, 115];
const S_5_22: &[u8] = &[102, 117, 108, 110, 101, 115, 115];
const S_5_23: &[u8] = &[111, 117, 115, 110, 101, 115, 115];
const S_5_3: &[u8] = &[108, 105];
const S_5_4: &[u8] = &[98, 108, 105];
const S_5_5: &[u8] = &[97, 98, 108, 105];
const S_5_6: &[u8] = &[97, 108, 108, 105];
const S_5_7: &[u8] = &[102, 117, 108, 108, 105];
const S_5_8: &[u8] = &[108, 101, 115, 115, 108, 105];
const S_5_9: &[u8] = &[111, 117, 115, 108, 105];
const S_6: &[u8] = &[101];
const S_6_0: &[u8] = &[105, 99, 97, 116, 101];
const S_6_1: &[u8] = &[97, 116, 105, 118, 101];
const S_6_2: &[u8] = &[97, 108, 105, 122, 101];
const S_6_3: &[u8] = &[105, 99, 105, 116, 105];
const S_6_4: &[u8] = &[105, 99, 97, 108];
const S_6_5: &[u8] = &[116, 105, 111, 110, 97, 108];
const S_6_6: &[u8] = &[97, 116, 105, 111, 110, 97, 108];
const S_6_7: &[u8] = &[102, 117, 108];
const S_6_8: &[u8] = &[110, 101, 115, 115];
const S_7: &[u8] = &[101];
const S_7_0: &[u8] = &[105, 99];
const S_7_1: &[u8] = &[97, 110, 99, 101];
const S_7_10: &[u8] = &[105, 115, 109];
const S_7_11: &[u8] = &[105, 111, 110];
const S_7_12: &[u8] = &[101, 114];
const S_7_13: &[u8] = &[111, 117, 115];
const S_7_14: &[u8] = &[97, 110, 116];
const S_7_15: &[u8] = &[101, 110, 116];
const S_7_16: &[u8] = &[109, 101, 110, 116];
const S_7_17: &[u8] = &[101, 109, 101, 110, 116];
const S_7_2: &[u8] = &[101, 110, 99, 101];
const S_7_3: &[u8] = &[97, 98, 108, 101];
const S_7_4: &[u8] = &[105, 98, 108, 101];
const S_7_5: &[u8] = &[97, 116, 101];
const S_7_6: &[u8] = &[105, 118, 101];
const S_7_7: &[u8] = &[105, 122, 101];
const S_7_8: &[u8] = &[105, 116, 105];
const S_7_9: &[u8] = &[97, 108];
const S_8: &[u8] = &[105];
const S_8_0: &[u8] = &[101];
const S_8_1: &[u8] = &[108];
const S_9: &[u8] = &[116, 105, 111, 110];
const S_9_0: &[u8] = &[115, 117, 99, 99, 101, 101, 100];
const S_9_1: &[u8] = &[112, 114, 111, 99, 101, 101, 100];
const S_9_2: &[u8] = &[101, 120, 99, 101, 101, 100];
const S_9_3: &[u8] = &[99, 97, 110, 110, 105, 110, 103];
const S_9_4: &[u8] = &[105, 110, 110, 105, 110, 103];
const S_9_5: &[u8] = &[101, 97, 114, 114, 105, 110, 103];
const S_9_6: &[u8] = &[104, 101, 114, 114, 105, 110, 103];
const S_9_7: &[u8] = &[111, 117, 116, 105, 110, 103];

const G_AEO: &[u8] = &[17, 64];
const G_V: &[u8] = &[17, 65, 16, 1];
const G_V_WXY: &[u8] = &[1, 17, 65, 208, 1];
const G_VALID_LI: &[u8] = &[55, 141, 2];

const A_0: &[Among] = &[
    Among {
        s: S_0_0,
        substring_i: -1,
        result: -1,
    },
    Among {
        s: S_0_1,
        substring_i: -1,
        result: -1,
    },
    Among {
        s: S_0_2,
        substring_i: -1,
        result: -1,
    },
];
const A_1: &[Among] = &[
    Among {
        s: S_1_0,
        substring_i: -1,
        result: 1,
    },
    Among {
        s: S_1_1,
        substring_i: 0,
        result: 1,
    },
    Among {
        s: S_1_2,
        substring_i: -1,
        result: 1,
    },
];
const A_2: &[Among] = &[
    Among {
        s: S_2_0,
        substring_i: -1,
        result: 2,
    },
    Among {
        s: S_2_1,
        substring_i: -1,
        result: 3,
    },
    Among {
        s: S_2_2,
        substring_i: 1,
        result: 2,
    },
    Among {
        s: S_2_3,
        substring_i: 1,
        result: 1,
    },
    Among {
        s: S_2_4,
        substring_i: 1,
        result: -1,
    },
    Among {
        s: S_2_5,
        substring_i: 1,
        result: -1,
    },
];
const A_3: &[Among] = &[
    Among {
        s: &[],
        substring_i: -1,
        result: 3,
    },
    Among {
        s: S_3_1,
        substring_i: 0,
        result: 2,
    },
    Among {
        s: S_3_2,
        substring_i: 0,
        result: 2,
    },
    Among {
        s: S_3_3,
        substring_i: 0,
        result: 2,
    },
    Among {
        s: S_3_4,
        substring_i: 0,
        result: 2,
    },
    Among {
        s: S_3_5,
        substring_i: 0,
        result: 1,
    },
    Among {
        s: S_3_6,
        substring_i: 0,
        result: 2,
    },
    Among {
        s: S_3_7,
        substring_i: 0,
        result: 2,
    },
    Among {
        s: S_3_8,
        substring_i: 0,
        result: 2,
    },
    Among {
        s: S_3_9,
        substring_i: 0,
        result: 2,
    },
    Among {
        s: S_3_10,
        substring_i: 0,
        result: 1,
    },
    Among {
        s: S_3_11,
        substring_i: 0,
        result: 2,
    },
    Among {
        s: S_3_12,
        substring_i: 0,
        result: 1,
    },
];
const A_4: &[Among] = &[
    Among {
        s: S_4_0,
        substring_i: -1,
        result: 2,
    },
    Among {
        s: S_4_1,
        substring_i: 0,
        result: 1,
    },
    Among {
        s: S_4_2,
        substring_i: -1,
        result: 2,
    },
    Among {
        s: S_4_3,
        substring_i: -1,
        result: 2,
    },
    Among {
        s: S_4_4,
        substring_i: 3,
        result: 1,
    },
    Among {
        s: S_4_5,
        substring_i: -1,
        result: 2,
    },
];
const A_5: &[Among] = &[
    Among {
        s: S_5_0,
        substring_i: -1,
        result: 3,
    },
    Among {
        s: S_5_1,
        substring_i: -1,
        result: 2,
    },
    Among {
        s: S_5_2,
        substring_i: -1,
        result: 13,
    },
    Among {
        s: S_5_3,
        substring_i: -1,
        result: 15,
    },
    Among {
        s: S_5_4,
        substring_i: 3,
        result: 12,
    },
    Among {
        s: S_5_5,
        substring_i: 4,
        result: 4,
    },
    Among {
        s: S_5_6,
        substring_i: 3,
        result: 8,
    },
    Among {
        s: S_5_7,
        substring_i: 3,
        result: 9,
    },
    Among {
        s: S_5_8,
        substring_i: 3,
        result: 14,
    },
    Among {
        s: S_5_9,
        substring_i: 3,
        result: 10,
    },
    Among {
        s: S_5_10,
        substring_i: 3,
        result: 5,
    },
    Among {
        s: S_5_11,
        substring_i: -1,
        result: 8,
    },
    Among {
        s: S_5_12,
        substring_i: -1,
        result: 12,
    },
    Among {
        s: S_5_13,
        substring_i: -1,
        result: 11,
    },
    Among {
        s: S_5_14,
        substring_i: -1,
        result: 1,
    },
    Among {
        s: S_5_15,
        substring_i: 14,
        result: 7,
    },
    Among {
        s: S_5_16,
        substring_i: -1,
        result: 8,
    },
    Among {
        s: S_5_17,
        substring_i: -1,
        result: 7,
    },
    Among {
        s: S_5_18,
        substring_i: 17,
        result: 6,
    },
    Among {
        s: S_5_19,
        substring_i: -1,
        result: 6,
    },
    Among {
        s: S_5_20,
        substring_i: -1,
        result: 7,
    },
    Among {
        s: S_5_21,
        substring_i: -1,
        result: 11,
    },
    Among {
        s: S_5_22,
        substring_i: -1,
        result: 9,
    },
    Among {
        s: S_5_23,
        substring_i: -1,
        result: 10,
    },
];
const A_6: &[Among] = &[
    Among {
        s: S_6_0,
        substring_i: -1,
        result: 4,
    },
    Among {
        s: S_6_1,
        substring_i: -1,
        result: 6,
    },
    Among {
        s: S_6_2,
        substring_i: -1,
        result: 3,
    },
    Among {
        s: S_6_3,
        substring_i: -1,
        result: 4,
    },
    Among {
        s: S_6_4,
        substring_i: -1,
        result: 4,
    },
    Among {
        s: S_6_5,
        substring_i: -1,
        result: 1,
    },
    Among {
        s: S_6_6,
        substring_i: 5,
        result: 2,
    },
    Among {
        s: S_6_7,
        substring_i: -1,
        result: 5,
    },
    Among {
        s: S_6_8,
        substring_i: -1,
        result: 5,
    },
];
const A_7: &[Among] = &[
    Among {
        s: S_7_0,
        substring_i: -1,
        result: 1,
    },
    Among {
        s: S_7_1,
        substring_i: -1,
        result: 1,
    },
    Among {
        s: S_7_2,
        substring_i: -1,
        result: 1,
    },
    Among {
        s: S_7_3,
        substring_i: -1,
        result: 1,
    },
    Among {
        s: S_7_4,
        substring_i: -1,
        result: 1,
    },
    Among {
        s: S_7_5,
        substring_i: -1,
        result: 1,
    },
    Among {
        s: S_7_6,
        substring_i: -1,
        result: 1,
    },
    Among {
        s: S_7_7,
        substring_i: -1,
        result: 1,
    },
    Among {
        s: S_7_8,
        substring_i: -1,
        result: 1,
    },
    Among {
        s: S_7_9,
        substring_i: -1,
        result: 1,
    },
    Among {
        s: S_7_10,
        substring_i: -1,
        result: 1,
    },
    Among {
        s: S_7_11,
        substring_i: -1,
        result: 2,
    },
    Among {
        s: S_7_12,
        substring_i: -1,
        result: 1,
    },
    Among {
        s: S_7_13,
        substring_i: -1,
        result: 1,
    },
    Among {
        s: S_7_14,
        substring_i: -1,
        result: 1,
    },
    Among {
        s: S_7_15,
        substring_i: -1,
        result: 1,
    },
    Among {
        s: S_7_16,
        substring_i: 15,
        result: 1,
    },
    Among {
        s: S_7_17,
        substring_i: 16,
        result: 1,
    },
];
const A_8: &[Among] = &[
    Among {
        s: S_8_0,
        substring_i: -1,
        result: 1,
    },
    Among {
        s: S_8_1,
        substring_i: -1,
        result: 2,
    },
];
const A_9: &[Among] = &[
    Among {
        s: S_9_0,
        substring_i: -1,
        result: -1,
    },
    Among {
        s: S_9_1,
        substring_i: -1,
        result: -1,
    },
    Among {
        s: S_9_2,
        substring_i: -1,
        result: -1,
    },
    Among {
        s: S_9_3,
        substring_i: -1,
        result: -1,
    },
    Among {
        s: S_9_4,
        substring_i: -1,
        result: -1,
    },
    Among {
        s: S_9_5,
        substring_i: -1,
        result: -1,
    },
    Among {
        s: S_9_6,
        substring_i: -1,
        result: -1,
    },
    Among {
        s: S_9_7,
        substring_i: -1,
        result: -1,
    },
];
const A_10: &[Among] = &[
    Among {
        s: S_10_0,
        substring_i: -1,
        result: -1,
    },
    Among {
        s: S_10_1,
        substring_i: -1,
        result: -1,
    },
    Among {
        s: S_10_2,
        substring_i: -1,
        result: -1,
    },
    Among {
        s: S_10_3,
        substring_i: -1,
        result: -1,
    },
    Among {
        s: S_10_4,
        substring_i: -1,
        result: 3,
    },
    Among {
        s: S_10_5,
        substring_i: -1,
        result: 9,
    },
    Among {
        s: S_10_6,
        substring_i: -1,
        result: 7,
    },
    Among {
        s: S_10_7,
        substring_i: -1,
        result: -1,
    },
    Among {
        s: S_10_8,
        substring_i: -1,
        result: 6,
    },
    Among {
        s: S_10_9,
        substring_i: -1,
        result: 4,
    },
    Among {
        s: S_10_10,
        substring_i: -1,
        result: -1,
    },
    Among {
        s: S_10_11,
        substring_i: -1,
        result: 10,
    },
    Among {
        s: S_10_12,
        substring_i: -1,
        result: 11,
    },
    Among {
        s: S_10_13,
        substring_i: -1,
        result: 2,
    },
    Among {
        s: S_10_14,
        substring_i: -1,
        result: 1,
    },
    Among {
        s: S_10_15,
        substring_i: -1,
        result: -1,
    },
    Among {
        s: S_10_16,
        substring_i: -1,
        result: 5,
    },
    Among {
        s: S_10_17,
        substring_i: -1,
        result: 8,
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stems_match_postgresql() {
        // Ground truth generated by `to_tsvector('english', word)` on
        // PostgreSQL 18.4 over a sample of the web2 word list.
        let corpus = include_str!("english_stem_corpus.tsv");
        let mut checked = 0;
        for line in corpus.lines() {
            let (word, expected) = line.split_once('\t').expect("word<TAB>stem");
            assert_eq!(stem(word), expected, "stemming {word:?}");
            checked += 1;
        }
        assert!(checked > 500, "the corpus should not shrink");
    }

    /// Dumps stems for a word list, for comparison against PostgreSQL.
    /// Run with `cargo test -p nodus_executor dump_stems -- --ignored`.
    #[test]
    #[ignore]
    fn dump_stems_for_parity() {
        let Ok(words) = std::fs::read_to_string("/usr/share/dict/words") else {
            eprintln!("no /usr/share/dict/words");
            return;
        };
        let mut out = String::new();
        for word in words.lines() {
            if word.len() < 3 || !word.bytes().all(|b| b.is_ascii_alphabetic()) {
                continue;
            }
            let lower = word.to_ascii_lowercase();
            out.push_str(&lower);
            out.push('\t');
            out.push_str(&stem(&lower));
            out.push('\n');
        }
        std::fs::write("/tmp/stem_out.txt", out).expect("wrote the dump");
        eprintln!("wrote /tmp/stem_out.txt");
    }
}
