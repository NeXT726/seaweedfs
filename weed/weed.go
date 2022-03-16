package main

import (
	"embed"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"strings"
	"sync"
	"text/template"
	"time"
	"unicode"
	"unicode/utf8"

	weed_server "github.com/chrislusf/seaweedfs/weed/server"
	flag "github.com/chrislusf/seaweedfs/weed/util/fla9"

	"github.com/chrislusf/seaweedfs/weed/command"
	"github.com/chrislusf/seaweedfs/weed/glog"
)

var IsDebug *bool

//command指令列表
var commands = command.Commands

var exitStatus = 0
var exitMu sync.Mutex

func setExitStatus(n int) {
	exitMu.Lock()
	if exitStatus < n {
		exitStatus = n
	}
	exitMu.Unlock()
}

//go:embed static
var static embed.FS

func init() {
	weed_server.StaticFS, _ = fs.Sub(static, "static")
}

func main() {
	//32G，保存运行log
	glog.MaxSize = 1024 * 1024 * 32
	//随机数函数的种子
	rand.Seed(time.Now().UnixNano())
	//TODO：：打印使用规范
	flag.Usage = usage

	//自动补全函数
	if command.AutocompleteMain(commands) {
		return
	}

	//解析参数
	flag.Parse()

	//剩余的没有提前注册的参数，无法被上面的parse解析
	args := flag.Args()
	if len(args) < 1 {
		usage()
	}

	//weed help的指令解析
	if args[0] == "help" {
		help(args[1:])
		for _, cmd := range commands {
			if len(args) >= 2 && cmd.Name() == args[1] && cmd.Run != nil {
				fmt.Fprintf(os.Stderr, "Default Parameters:\n")
				cmd.Flag.PrintDefaults()
			}
		}
		return
	}

	//其他weed指令
	for _, cmd := range commands {
		if cmd.Name() == args[0] && cmd.Run != nil {
			cmd.Flag.Usage = func() { cmd.Usage() }
			cmd.Flag.Parse(args[1:])
			args = cmd.Flag.Args()
			IsDebug = cmd.IsDebug
			//执行该指令的函数
			if !cmd.Run(cmd, args) {
				fmt.Fprintf(os.Stderr, "\n")
				cmd.Flag.Usage()
				fmt.Fprintf(os.Stderr, "Default Parameters:\n")
				cmd.Flag.PrintDefaults()
			}
			exit()
			return
		}
	}

	fmt.Fprintf(os.Stderr, "weed: unknown subcommand %q\nRun 'weed help' for usage.\n", args[0])
	setExitStatus(2)
	exit()
}

var usageTemplate = `
SeaweedFS: store billions of files and serve them fast!

Usage:

	weed command [arguments]

The commands are:
{{range .}}{{if .Runnable}}
    {{.Name | printf "%-11s"}} {{.Short}}{{end}}{{end}}

Use "weed help [command]" for more information about a command.

`

var helpTemplate = `{{if .Runnable}}Usage: weed {{.UsageLine}}
{{end}}
  {{.Long}}
`

// tmpl executes the given template text on data, writing the result to w.
//tmpl 在数据上执行给定的模板文本，将结果写入 w
//DJLTODO：template模块解析输出，需要具体学习
func tmpl(w io.Writer, text string, data interface{}) {
	t := template.New("top")
	t.Funcs(template.FuncMap{"trim": strings.TrimSpace, "capitalize": capitalize})
	template.Must(t.Parse(text))
	if err := t.Execute(w, data); err != nil {
		panic(err)
	}
}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	r, n := utf8.DecodeRuneInString(s)
	return string(unicode.ToTitle(r)) + s[n:]
}

func printUsage(w io.Writer) {
	tmpl(w, usageTemplate, commands)
}

func usage() {
	printUsage(os.Stderr)
	fmt.Fprintf(os.Stderr, "For Logging, use \"weed [logging_options] [command]\". The logging options are:\n")
	flag.PrintDefaults()
	os.Exit(2)
}

// help implements the 'help' command.
func help(args []string) {
	if len(args) == 0 {
		printUsage(os.Stdout)
		// not exit 2: succeeded at 'weed help'.
		return
	}
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "usage: weed help command\n\nToo many arguments given.\n")
		os.Exit(2) // failed at 'weed help'
	}

	arg := args[0]

	for _, cmd := range commands {
		if cmd.Name() == arg {
			tmpl(os.Stdout, helpTemplate, cmd)
			// not exit 2: succeeded at 'weed help cmd'.
			return
		}
	}

	fmt.Fprintf(os.Stderr, "Unknown help topic %#q.  Run 'weed help'.\n", arg)
	os.Exit(2) // failed at 'weed help cmd'
}

var atexitFuncs []func()

func atexit(f func()) {
	atexitFuncs = append(atexitFuncs, f)
}

func exit() {
	for _, f := range atexitFuncs {
		f()
	}
	os.Exit(exitStatus)
}

func debug(params ...interface{}) {
	glog.V(4).Infoln(params...)
}
