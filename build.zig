const std = @import("std");
const builtin = @import("builtin");

// Although this function looks imperative, note that its job is to
// declaratively construct a build graph that will be executed by an external
// runner.
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const clap = b.dependency("clap", .{
        .target = target,
        .optimize = optimize,
    });

    const lib = b.addStaticLibrary(.{
        .name = "nomad",
        .root_source_file = .{ .src_path = .{ .owner = b, .sub_path = "src/nomad.zig" } },
        .target = target,
        .optimize = optimize,
    });

    // For pthread
    lib.linkLibC();
    b.installArtifact(lib);

    const exe = b.addExecutable(.{
        .name = "nomad",
        .root_source_file = .{ .src_path = .{ .owner = b, .sub_path = "src/repl.zig" } },
        .target = target,
        .optimize = optimize,
    });

    exe.root_module.addImport("clap", clap.module("clap"));

    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run:repl", "Run the nomad REPL");
    run_step.dependOn(&run_cmd.step);

    const lib_unit_tests = b.addTest(.{
        .root_source_file = .{ .src_path = .{ .owner = b, .sub_path = "src/nomad.zig" } },
        .target = target,
        .optimize = optimize,
    });

    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);

    const exe_unit_tests = b.addTest(.{
        .root_source_file = .{ .src_path = .{ .sub_path = "src/repl.zig", .owner = b } },
        .target = target,
        .optimize = optimize,
    });

    const run_exe_unit_tests = b.addRunArtifact(exe_unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_unit_tests.step);
    test_step.dependOn(&run_exe_unit_tests.step);
}
