const std = @import("std");
const threading = @cImport({
    @cInclude("pthread.h");
});

const Self = @This();

pub const Status = enum(u8) {
    done,
    retry,
    failed,
};

pub const Task = struct {
    name: []const u8,
    method: *const fn () Status,
};

pub fn InternalQueue(comptime Child: type) type {
    return struct {
        const This = @This();
        const Node = struct {
            data: Child,
            next: ?*Node,
        };
        allocator: std.mem.Allocator,
        start: ?*Node,
        end: ?*Node,

        pub fn init(allocator: std.mem.Allocator) This {
            return This{
                .allocator = allocator,
                .start = null,
                .end = null,
            };
        }
        pub fn enqueue(this: *This, value: Child) !void {
            const node = try this.allocator.create(Node);
            node.* = .{ .data = value, .next = null };
            if (this.end) |end| end.next = node //
            else this.start = node;
            this.end = node;
        }
        pub fn dequeue(this: *This) ?Child {
            const _start = this.start orelse return null;
            defer this.allocator.destroy(_start);
            if (_start.next) |next|
                this.start = next
            else {
                this.start = null;
                this.end = null;
            }
            return _start.data;
        }
    };
}

const InternalList = InternalQueue(Task);

name: []const u8,
tasks: InternalList,
allocator: std.mem.Allocator,

pub fn init(allocator: std.mem.Allocator, name: []const u8) Self {
    const items = InternalList.init(allocator);

    return Self{
        .name = name,
        .tasks = items,
        .allocator = allocator,
    };
}

fn _thread_loop(self: Self) void {
    var tasks = self.tasks;
    while (true) {
        if (tasks.dequeue()) |item| {
            switch (item.method()) {
                .done => std.log.info("Queue({s}): '{s}' done", .{ self.name, item.name }),
                .failed => std.log.info("Queue({s}): '{s}' failed", .{ self.name, item.name }),
                .retry => {
                    std.log.info("Queue({s}): '{s}' re-queued", .{ self.name, item.name });
                    tasks.enqueue(item) catch unreachable; // Very risky but this shouldn't happen unless you are out of ram
                },
            }
        }
    }
}

pub fn start(self: Self) !void {
    const thread = try std.Thread.spawn(.{}, _thread_loop, .{self});

    thread.join();
}
