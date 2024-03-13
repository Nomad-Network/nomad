const std = @import("std");

pub fn fourBytesToU32(bytes: *const [4]u8) u32 {
    const number: u32 = @bitCast(bytes.*);
    return std.mem.nativeTo(u32, number, .little);
}

pub fn eightBytesToU64(bytes: *const [8]u8) u64 {
    const number: u64 = @bitCast(bytes.*);
    return std.mem.nativeTo(u64, number, .little);
}

pub fn sixteenBytesToU128(bytes: *const [16]u8) u128 {
    const number: u128 = @bitCast(bytes.*);
    return std.mem.nativeTo(u128, number, .little);
}

pub fn u32ToFourBytes(number: u32, endianess: std.builtin.Endian) [4]u8 {
    const little_number = std.mem.nativeTo(u32, number, endianess);
    const slice: [4]u8 = @bitCast(little_number);

    return slice;
}

pub fn u128ToSixteenBytes(number: u128, endianess: std.builtin.Endian) [16]u8 {
    const little_number = std.mem.nativeTo(u128, number, endianess);
    const slice: [16]u8 = @bitCast(little_number);

    return slice;
}

pub fn u64ToEightBytes(number: u64, endianess: std.builtin.Endian) [8]u8 {
    const little_number = std.mem.nativeTo(u64, number, endianess);
    const mem: [8]u8 = @bitCast(little_number);

    return mem;
}

pub fn sliceToU64(slice: []u8) u64 {
    if (slice.len != 8) return 0;
    std.mem.reverse(u8, slice);
    var r: u64 = 0;

    for (slice, 0..) |v, i| {
        const up_v: u64 = v;
        const shift_amt: u6 = @intCast(i);
        r += @shlExact(up_v, (7 - shift_amt) * 8);
    }

    return r;
}

pub fn sliceToSizedArray(comptime T: type, comptime size: usize, src: []const T, dst: *const [size]T) *const [size]T {
    var i: u64 = 0;
    var tmp_dst = dst.*;

    while (i < size) {
        tmp_dst[i] = src[i];
        i += 1;
    }

    const r = tmp_dst;
    return &r;
}

test "fourBytesToU32" {
    const four_bytes = [4]u8{ 0x10, 0x10, 0x10, 0x30 };
    const expected: u32 = 0x30101010; // little endian

    std.testing.expect(fourBytesToU32(four_bytes) == expected);
}

test "eightBytesToU64" {
    const eight_bytes = [8]u6{ 0x0, 0x0, 0x0, 0x0, 0x10, 0x20, 0x30, 0x40 };
    const expected: u64 = 0x40302010000000;

    std.testing.expect(eightBytesToU64(eight_bytes) == expected);
}

test "sixteenBytesToU128" {
    const sixteen_bytes: [16]u8 = "a" ** 16;
    const expected: u128 = 0x61_61_61_61_61_61_61_61_61_61_61_61_61_61_61_61;

    std.testing.expect(sixteenBytesToU128(sixteen_bytes) == expected);
}

test "u128ToSixteenBytes" {
    const expected: [16]u8 = "a" ** 16;
    const sixteen_a: u128 = 0x61_61_61_61_61_61_61_61_61_61_61_61_61_61_61_61;

    std.testing.expect(u128ToSixteenBytes(sixteen_a) == expected);
}
