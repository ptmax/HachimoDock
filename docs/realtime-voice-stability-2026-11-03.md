# 实时语音稳定性复盘（2026-11-03）

> 范围：ESP32-P4 固件、HachimoDock App、DSH 实时对话、USB CDC、macOS 后台调度。
>
> 环境：P4 `0.7.103-p4-r2`（rev 3.2）、App `0.1.168`、macOS。

## 1. AEC3 HIGH_PERF 导致 SW2 重启

### 现象

- 长按 SW2，实时对话 UI 刚出现，P4 panic 并重启；
- App 不退出；
- 串口 backtrace：

```text
Guru Meditation: Load access fault
esp_aec3_dlfft_process
dl_esp32p4_sr_memcpy
audio_task
```

### 根因

- AEC3 内部句柄偏移 `+116` 持有 SIMD 源指针；
- Wi‑Fi / ESP‑Hosted 任务占用 HP SRAM 后，AEC destroy/recreate 把它分配到 LP RAM：

```text
0x50108000 起的 LPRAM
CONFIG_ESP_SYSTEM_ALLOW_RTC_FAST_MEM_AS_HEAP=1
```

- `esp_ptr_internal()` 认为它是内部 RAM，但 AEC3 的 128-bit `dl_esp32p4_sr_memcpy` 不能访问 LPRAM；
- 启动时 AEC 拿到合法 HP SRAM，所以不崩；重建后拿到 `0x501084a0`，首帧访问即 panic。

### 修复

- `CONFIG_ESP_SYSTEM_ALLOW_RTC_FAST_MEM_AS_HEAP` 关闭，禁止 LPRAM 进入通用 heap；
- AEC HIGH_PERF 改为单实例，新对话只 reset 队列/ring/chunk，不 destroy/recreate；
- 保持音频栈 8KB，不采用 16KB 掩盖；
- 保留 Wi‑Fi、PPA、high-perf AEC。

### 结论

16KB 栈只是改变堆布局，让 f116 恰好留在 HP SRAM；正式修复是让 AEC 永远不接触 LPRAM。

---

## 2. “客户端未连接” / 实时语音中断

### 现象

- 长回复播放到后段，语音突然中断；
- P4 显示“客户端未连接”；
- App 进程仍在，P4 没有重启；
- 日志：

```text
live_write_failed stage=audio_chunk
elapsedMs=426 writtenBytes=2304
P4 playback_underrun
```

### 根因分析

- 不是 App 崩溃，也不是 P4 panic；
- 是 App 到 P4 的 USB CDC 音频写入超时；
- App 实际使用 USB Serial/JTAG CDC，不是 native USB vendor bulk；
- 最初提高 native TinyUSB 任务优先级，打错了路径；
- 真正相关的是 P4 `usb_rx_task` / `uart_rx_task`，原先优先级只有 10，低于音频 18、渲染 17。

### 已做修复

1. P4 USB/UART transport RX 任务优先级 `10 -> 20`；
2. macOS App Nap 防护：App 启动 `caffeinate -imsu -w <pid>`，避免窗口被遮挡后 USB 写线程被节流；
3. live audio 写恢复：
   - `RECOVERY_DEADLINE 350ms -> 1500ms`
   - `MAX_RECOVERY_RETRIES 3 -> 8`

### 状态

- App Nap 修复后“客户端未连接”明显减少；
- 长回复压力下仍复现过写入超时，说明 P4 USB CDC 在持续音频流下仍可能停止 drain；
- 当前修复是缓解，不是最终证明；
- 下一步需抓 P4 USJ/USB 任务状态、RX 队列、是否存在长临界区/日志 TX 竞争。

---

## 3. 偶发 TTS 句子重复/半句后重播

### 现象

- Buddy 一句话先播一小段；
- 状态切换；
- 又完整播放一遍；
- bubble 快速变化。

### 已定位的机制

日志中出现：

```text
barge_confirmed state=speaking
barge_in
playback_flushed
新 turn
```

这是 barge-in 判定后 Flush 旧回复，再开始新回复，不是 USB 重复发送同一帧。

### 已做修复

- TTS 回调绑定合成开始时的 player epoch；
- Flush 后旧 TTS 回调的 PCM（即使晚到）会被丢弃，不会继承新 epoch；
- barge 回声判断：
  - 精确包含回复文本时拒绝；
  - 增加字符 bigram 相似度过滤；
  - 显式命令：`停/停止/等一下/等等/先别/暂停/换歌/下一首/取消`；
  - 显式命令有 8 个 voiced frames 就立即打断；
  - 其他语音保持原始 2 字门槛，但增加回声过滤。

### 教训

- barge 门槛不能为了压误触发无限抬高；抬到 6 字会让“等一下”失效；
- 原设计是 2 字门槛、终端 8 frames、非终端 12 frames + 2 次修订；
- 需要区分“用户插话”和“播放回声”，不能只靠文本长度。

---

## 4. macOS App Nap / 后台节流

### 现象

- 只在 HachimiDock 被 DSH 遮挡/后台时出现 USB 音频写入超时；
- App 前台/`caffeinate` 保护时不出现。

### 修复

- macOS 启动时绑定自身 PID 的 `caffeinate -imsu -w`；
- 仅阻止空闲/App Nap，不阻止显示器睡眠。

### 教训

- WebView 定时器之外，Rust 线程同样会被 App Nap/后台调度影响；
- 排查后台问题要做前台/后台对照，不要只看 USB 错误码。

---

## 5. 审批卡片不显示（待办）

### 现状

- Host `pendingApprovals=0` 时 P4 不显示卡片是正常的；
- 当前会话的 approval prompts 实际被自动放行/关闭，无法产生 pending；
- 子代理不能升级权限，也不能触发交互审批；
- 需要在新会话/真正询问模式下产生 pending，再抓：
  - Host approval SSE；
  - App `dsh_approval` `/events`；
  - USB `approval/request`；
  - P4 approval state/renderer。

### 未完成

- 尚未证明是 P4 renderer、App relay 还是 Host policy 的问题。

---

## 6. 亮度（待办）

- 屏幕原理图：`BLC -> R1 -> Q1`，Q1 低边开关 LED 背光；
- 没有独立亮度控制 IC；
- 理论上 BLC 可 PWM，但需要确认主控 GPIO 和转接板 pinout；
- 当前 GPIO26/LEDC 实测无变化，不能宣称支持调光。

---

## 7. 构建与刷写经验

- Debug App + Vite `127.0.0.1:4173` 会被 Agent Bus 拒绝，session feed 为 0；
- Release 构建必须避免 `--cfg dev`，否则仍加载 devUrl；
- 裸 `cargo build --release` 与 Tauri CLI feature set 不同，会重复编译；
- esptool 走 P4 USB CDC 时 stub + 高速率易出现 `Invalid head of packet`；
- 稳定恢复方式：

```bash
esptool --chip esp32p4 -p <port> -b 115200 --no-stub \
  write-flash 0x10000 firmware.bin
```

- 只写 app 时若 otadata 仍指向另一分区，可能启动旧固件；必要时写 `ota_data_initial.bin`；
- 原厂 app 资源/任务对比非常有用：

```text
原厂: 17 tasks, 153KB internal free
我们: 27 tasks,  65KB internal free
```

---

## 8. 分支和提交

```text
fix/dsh-full-duplex-aec     10b9579  LPRAM + AEC 单实例
fix/p4-usb-scheduling       af6a4d0  CDC/native USB 任务优先级
fix/macos-app-nap           c0cfd6d  App Nap 防护
fix/realtime-tts-epoch      a7c34bc  TTS epoch
fix/realtime-barge-echo     12669d5  barge 回声过滤（历史）
integration/autonomous-test 9bebec0  最新语音稳定性修复
```

最终快照分支：`fix/voice-realtime-stability`。

## 9. 最重要的经验

1. **不要用加栈解决内存分配问题**；先确认分配到的地址是否可用。
2. **`esp_ptr_internal()` 不等于 AEC SIMD 可访问**，LPRAM 是特殊区域。
3. **“没有 ALLOC FAILED”不代表指针有效**；要检查返回地址范围和用途。
4. **原厂固件是最好的对照组**：任务数、内部 RAM、配置、UI/字体都可比。
5. **USB 问题要确认走的是 CDC 还是 native USB**，不要改错任务。
6. **macOS App Nap 会让后台 Rust 线程迟到**；需要显式 activity/断言。
7. **barge 门槛是体验与误触发权衡**：显式命令立即放行，其他语音保留低声学门槛。
8. **TTS 回调要绑定 epoch**，否则 Flush 后旧音频会混入新回复。
9. **临时诊断钩子必须清理**，刷写 marker/raw log 会伪装成产品 bug。
10. **每个修复都要区分“缓解”与“根因”**；当前 USB CDC 长回复仍有未闭环风险。
