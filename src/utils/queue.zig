const std = @import("std");

pub fn Queue(comptime Context: type) type {
    return struct {
        const Self = @This();

        const InternalList = InternalQueue(Task);

        pub const Status = enum(u8) {
            done,
            retry,
            failed,
        };

        pub const Task = struct {
            name: []const u8,
            method: *const fn (*Context) Status,
            ctx: ?*Context,
        };

        name: []const u8,
        tasks: InternalList,
        allocator: std.mem.Allocator,
        logging_enabled: bool,
        context: *Context,
        thread: ?std.Thread,

        pub fn init(allocator: std.mem.Allocator, name: []const u8, ctx: Context) Self {
            const items = InternalList.init(allocator);

            return Self{
                .name = name,
                .tasks = items,
                .allocator = allocator,
                .logging_enabled = true,
                .context = @constCast(&ctx),
                .thread = null,
            };
        }

        fn log(self: Self, comptime format: []const u8, extra: []const u8) void {
            if (self.logging_enabled) std.log.info("Queue({s}): " ++ format, .{ self.name, extra });
        }

        fn _thread_loop(self: *Self) void {
            var tasks = self.tasks;
            while (true) {
                if (tasks.dequeue()) |item| {
                    switch (item.method(item.ctx orelse self.context)) {
                        .done => self.log("'{s}' done", item.name),
                        .failed => self.log("'{s}' failed", item.name),
                        .retry => {
                            self.log("'{s}' re-queued", item.name);
                            tasks.enqueue(item) catch unreachable; // Very risky but this shouldn't happen unless you are out of ram
                        },
                    }
                }
            }
        }

        pub fn start(self: *Self) !void {
            self.thread = try std.Thread.spawn(.{}, _thread_loop, .{self});
        }
    };
}

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
