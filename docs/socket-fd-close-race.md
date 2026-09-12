# mux 关闭与带宽统计之间的文件描述符竞态修复

记录日期：2026-09-13。

## 问题与发现过程

在 NPS 的客户端会话并发测试中，Go race detector 报告了数据竞争：一个协程正在读取 socket 文件描述符，另一个协程同时关闭该描述符。问题发生于 `nps-mux v0.0.35` 的非 Windows 实现。

最初只出现一次，随后 30 轮测试没有复现；扩大到 200 轮后再次捕获完整调用栈。因此，短期未复现只是没有遇到相同的协程执行顺序，不代表问题已消失。

此次是在 `TestConcurrentDisconnectAndLateActivation` 测试中观察到的竞态，不是线上已经确认的崩溃。测试让数据通道初始化、会话退出和通道关闭交错执行，暴露了 mux 原有的描述符生命周期问题。

## 两条发生冲突的调用链

读协程为统计带宽查询接收缓冲区大小：

```text
Mux.readSession
  → bandwidth.StartRead
  → bandwidth.calcBandWidth
  → sysGetSock
  → os.File.Fd
  → 读取内部 poll.FD.Sysfd
```

关闭协程释放带宽统计使用的文件描述符：

```text
Mux.closeWithReason 的关闭协程
  → bandwidth.Close
  → os.File.Close
  → internal/poll.FD.destroy
  → 修改内部 poll.FD.Sysfd
```

捕获的报告包含以下关键内容，省略具体地址和协程编号：

```text
WARNING: DATA RACE
Write:
  internal/poll.(*FD).destroy()
  os.(*File).Close()
  nps-mux.(*bandwidth).Close()
  nps-mux.(*Mux).closeWithReason.func2()

Previous read:
  os.(*File).fd()
  os.(*File).Fd()
  nps-mux.sysGetSock()
  nps-mux.(*bandwidth).calcBandWidth()
  nps-mux.(*bandwidth).StartRead()
  nps-mux.(*Mux).readSession.func2()
```

## 根因

`getConnFd` 通过 `TCPConn.File()` 或 `UDPConn.File()` 获取一个 `*os.File`，交给带宽统计使用。原来的查询代码如下：

```go
return syscall.GetsockoptInt(int(fd.Fd()), syscall.SOL_SOCKET, syscall.SO_RCVBUF)
```

与此同时，mux 关闭路径会调用 `bandwidth.Close()`，关闭同一个 `*os.File`。查询路径没有保护从取得描述符到完成 `getsockopt` 的整个时间段。

这存在两个问题：

1. 本次实际捕获的是 `Fd()` 读取内部描述符字段，与 `Close()` 销毁时修改该字段之间的数据竞争。
2. 即使先读到了整数描述符，后续系统调用执行前，它也可能已经关闭，甚至被操作系统分配给其他资源。这是生命周期上的潜在风险，本次测试没有证明发生过描述符复用。

`IsClose` 的原子检查不能解决这一问题。读协程可能先看到“未关闭”，随后关闭协程开始关闭文件，而读协程继续执行缓冲区查询。原子标志没有保护文件描述符的使用过程。

## 修复方式

修改文件：[sysGetsock_nowindows.go](../sysGetsock_nowindows.go)。

改用 `os.File.SyscallConn()` 获取 `syscall.RawConn`，并在 `Control` 回调中执行 `getsockopt`：

```go
raw, err := fd.SyscallConn()
if err != nil {
    return 0, err
}

var socketErr error
err = raw.Control(func(socket uintptr) {
    bufferSize, socketErr = syscall.GetsockoptInt(
        int(socket), syscall.SOL_SOCKET, syscall.SO_RCVBUF,
    )
})
if err != nil {
    return 0, err
}
return bufferSize, socketErr
```

`Control` 在回调执行期间持有描述符引用，使关闭操作无法在系统调用进行中销毁或回收该描述符。如果文件已经关闭，调用返回错误，不再使用一个未经生命周期保护的整数描述符。

错误分两层处理：`SyscallConn` / `Control` 自身的错误，以及回调内 `GetsockoptInt` 的错误。查询失败会沿用带宽统计现有的错误处理路径。

`fd == nil` 时仍返回原来的 5 MiB 默认缓冲区大小。此次不改变带宽计算公式、mux 协议、心跳间隔或超时阈值；修复仅涉及非 Windows 描述符查询路径。

## 测试与结果

新增回归测试：[sysGetsock_nowindows_test.go](../sysGetsock_nowindows_test.go) 中的 `TestSysGetSockConcurrentClose`。

测试使用本地 UDP socket 获取文件描述符，循环 100 轮，每轮先确认缓冲区大小查询有效，再并发执行重复查询和文件关闭，最后确认已关闭的描述符不能查询成功。查询与关闭交错时允许返回错误；检查目标是正常查询行为、关闭后行为以及 race detector 是否报告竞争。

专项测试命令，在 `nps-mux` 仓库执行：

```sh
go test -race ./... -run '^Test(SysGetSockConcurrentClose|CloseErrorKind)$' -count=1 -timeout=60s
```

结果：通过。

随后在 NPS 中使用临时 modfile，将 `ehang.io/nps-mux` 替换为本地修复后的仓库，执行 `go test -race ./bridge -count=200 -timeout=90s`，并构建 `./cmd/nps`。结果：200 轮 bridge 测试通过，未报告竞态，构建成功。

这些结果验证了本次修复覆盖的场景，不代表对整个 mux 库的所有并发路径作出了无竞态保证。未运行依赖 Docker、外部网络等环境的完整 mux 测试集。

## 与 vkey、连接池问题的关系

这次竞态发生在 mux 的带宽统计与关闭路径，和 NPS 是否允许同 vkey 的新主连接接管旧主连接是不同的问题。

此前设备重启后收到 `pNO1`，已由线上日志确认是旧数据通道占满池，随后因 TCP reset 关闭而释放容量。没有证据说明该池满现象由本次描述符竞态引起。修复描述符竞态本身也不会改变重复登录策略或消除旧会话等待时间。

## 发布与部署

本次验证构建临时使用本地修复版 mux，生成的 NPS 文件为 `/tmp/nps-first-client-wins`；没有部署线上。

截至本文记录时，NPS 仓库的依赖替换仍指向 `github.com/aohanhongzhi/nps-mux v0.0.35`。仅修改本地 mux 仓库，不会让普通的 NPS 构建自动包含修复。

正式发布需要将修复提交并发布新的 mux 版本，再更新 NPS 的依赖替换版本、重新测试和构建。开发验证也可以继续使用指向本地仓库的临时 modfile。不能仅凭 NPS 编译成功就认定已经包含此修复。
