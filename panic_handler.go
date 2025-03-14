package nps_mux

import (
	"fmt"
	"os"
	"runtime/debug"
	"time"
)

// PanicHandler https://blog.csdn.net/xia_xing/article/details/80597472
// 异常处理
func PanicHandler() {
	errs := recover()
	if errs == nil {
		return
	}

	name, err2 := os.Hostname()
	if err2 != nil {
		println(err2)
	}

	exeName := os.Args[0] //获取程序名称

	now := time.Now()  //获取当前时间
	pid := os.Getpid() //获取进程ID

	timeStr := now.Format("20060102150405")                                   //设定时间格式
	fname := fmt.Sprintf("%s-%d-%s-%v-dump.log", exeName, pid, timeStr, name) //保存错误信息文件名:程序名-进程ID-当前时间（年月日时分秒）
	fmt.Println("dump to file ", fname)

	f, err := os.Create(fname)
	if err != nil {
		return
	}
	defer f.Close()

	f.WriteString(fmt.Sprintf("%v\r\n", errs)) //输出panic信息
	f.WriteString("========\r\n")
	f.WriteString(string(debug.Stack())) //输出堆栈信息
}
