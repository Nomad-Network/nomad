const std = @import("std");

pub fn SteppedIterator(comptime T: type, comptime step: u64) type {
    return struct {
        const Self = @This();

        items: []T,
        index: usize = 0,

        pub fn next(self: *Self) ?[]T {
            const idx = self.index;

            const start = idx * step;
            const end = (idx + 1) * step;

            if (self.items.len < step) return null;

            const slice = self.items[start..end];

            self.items = self.items[end..];
            return slice;
        }
    };
}
