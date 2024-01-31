const std = @import("std");

pub fn fourBytesToU32(bytes: *const [4]u8) u32 {
    const number: u32 = @bitCast(bytes.*);
    return number;
}

pub fn eightBytesToU64(bytes: *const [8]u8) u64 {
    const number: u64 = @bitCast(bytes.*);
    return number;
}

pub fn sixteenBytesToU128(bytes: *const [16]u8) u128 {
    const number: u128 = @bitCast(bytes.*);
    return number;
}

pub fn u128ToSixteenBytes(number: u128, endianess: std.builtin.Endian) [16]u8 {
    const little_number = std.mem.nativeTo(u128, number, endianess);
    return [16]u8{
        little_number >> 120,
        (little_number >> 112) << 8,
        (little_number >> 104) << 16,
        (little_number >> 96) << 24,
        (little_number >> 88) << 32,
        (little_number >> 80) << 40,
        (little_number >> 72) << 48,
        (little_number >> 64) << 56,
        (little_number >> 56) << 64,
        (little_number >> 48) << 72,
        (little_number >> 40) << 80,
        (little_number >> 32) << 88,
        (little_number >> 24) << 96,
        (little_number >> 16) << 104,
        (little_number >> 8) << 112,
        little_number << 120,
    };
}

pub fn u64ToEightBytes(number: u64, endianess: std.builtin.Endian) [8]u8 {
    const little_number = std.mem.nativeTo(u64, number, endianess);
    const mem: [8]u8 = @bitCast(little_number);

    return mem;
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
