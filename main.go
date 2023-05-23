package main

import (
	"fmt"
	"time"

	"github.com/jackc/pgx"
)

type ConnOption struct {
	ip           string
	port         int
	databaseName string
	user         string
	password     string
}

type Options struct {
	addressMaster ConnOption
	slotName      string
	pluginName    string
	databaseName  string
}

type CDCController struct {
	addressSegments []ConnOption
	//we use this conn to get segment config, commit a transaction, get snapshot
	connMaster        *pgx.Conn //库函数返回的就是指针，我们只能用指针。。。
	connMasterCommand *pgx.Conn //get_snapshot_content, deliver_snapshot
	connSegments      []*pgx.Conn
	repConnMaster     *pgx.ReplicationConn //跑特殊参数的连接，普通连接用来传命令的
	repConnSegments   []*pgx.ReplicationConn
}

//就是说后边这些函数，哪些放到prepare里，哪些不放？

func (sc *CDCController) GetSegmentsAddress() {

	rows, err := sc.connMaster.Query("select * from gp_segment_configuration")
	if err != nil {
		fmt.Println(err)
	}
	for rows.Next() {
		var no int
		err := rows.Scan(&no)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("%d\n", no)
	}
}

//func (sc *CDCController) InitOneConn(address Address) (conn *pgx.Conn) { //要加error吗？
func InitOneConn(option ConnOption, isMaster bool) (conn *pgx.Conn) { //要加error吗？
	var config pgx.ConnConfig

	config.Host = option.ip
	config.Port = (uint16)(option.port)
	config.Database = option.databaseName //这地方看下怎么传进来
	config.User = option.user
	config.Password = option.password

	if !isMaster {
		if config.RuntimeParams == nil {
			config.RuntimeParams = make(map[string]string)
		}
		config.RuntimeParams["gp_role"] = "utility" //太假了，不用-c也好使。。。
	}

	conn, err := pgx.Connect(config)
	if err != nil {
		fmt.Println(err)
	}

	return
}

func InitOneRepConn(option ConnOption) (conn *pgx.ReplicationConn) { //要加error吗？
	var config pgx.ConnConfig

	config.Host = option.ip
	config.Port = (uint16)(option.port)
	config.Database = option.databaseName //这地方看下怎么传进来
	config.User = option.user
	config.Password = option.password

	conn, err := pgx.ReplicationConnect(config)
	fmt.Println(conn)
	fmt.Println(err)
	if err != nil {
		fmt.Println(err)
	}

	return
}

//先begin再导出快照，一个到master的连接够不够？如果只有一个，得保证这是到master连接的最后一个任务。
func (sc *CDCController) ExportSnapshot() (snapshotId string) {
	tx, err := sc.connMaster.Begin()
	if err != nil {
		fmt.Println(err)
	}

	//defer tx.Rollback()

	rows, err := tx.Query("select * from pg_export_snapshot();")
	if err != nil {
		fmt.Println(err)
	}

	var snapshot_id string
	for rows.Next() { //这个顺序是按我们预期的吗？
		err := rows.Scan(&snapshot_id) //传空，真是厉害
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("%s\n", snapshot_id)
	}

	/*
		err = tx.Commit()
		if err != nil {
			return err
		}
	*/
	/*
		rows, err := sc.connMaster.Query("select * from pg_export_snapshot();")
		if err != nil {
			fmt.Println(err)
		}

		var snapshot_id string

		for rows.Next() { //这个顺序是按我们预期的吗？
			err := rows.Scan(&snapshot_id) //传空，真是厉害
			if err != nil {
				fmt.Println(err)
			}
			fmt.Printf("%s\n", snapshot_id)
		}
	*/
	return snapshot_id
}

func (sc *CDCController) GetSnapshotContent(snapshotId string) (snapshotContent string) {

	var query string
	query += "select * from get_snapshot_content('"
	query += snapshotId
	query += "');"

	fmt.Println(query)
	//rows, err := sc.connMasterCommand.Query("select * from get_snapshot_content('%s');", snapshotId)

	rows, err := sc.connMasterCommand.Query(query)
	if err != nil {
		fmt.Println(err)
	}

	var content string
	for rows.Next() {
		err := rows.Scan(&content)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("content:%s\n", content)
	}

	return content
}

func (sc *CDCController) CommitOneTransaction() {
	tx, err := sc.connMasterCommand.Begin()
	if err != nil {
		fmt.Println(err)
	}

	_, err = tx.Exec("insert into tbl_2_columns values(700);")
	if err != nil {
		fmt.Println(err)
	}

	_, err = tx.Exec("insert into tbl_2_columns values(300);")
	if err != nil {
		fmt.Println(err)
	}

	_, err = tx.Exec("insert into tbl_2_columns values(600);")
	if err != nil {
		fmt.Println(err)
	}

	err = tx.Commit()
	if err != nil {
		fmt.Println(err)
	}
}

//理论上每次都要调用，我们的slot名字就一个，所以说如果slot存在，也没啥问题
func (sc *CDCController) CreateReplcationSlot() {

	err := sc.repConnMaster.CreateReplicationSlot("slot1", "test_decoding")
	if err != nil {
		fmt.Println(err)
	}

	for _, repConn := range sc.repConnSegments { //记住循环的写法
		err := repConn.CreateReplicationSlot("slot1", "test_decoding")
		if err != nil {
			fmt.Println(err)
		}
	}
}

func (sc *CDCController) DispatchSnapshot(snapshotContent string) {

	query := "select * from deliver_snapshot('"
	query += snapshotContent
	query += "');"

	/*
		_, err := sc.connMasterCommand.Query(query)
		if err != nil {
			fmt.Println(err)
		}
	*/
	//实在不行把回车替换掉。
	fmt.Println(query)

	//先拿7000跑通，master的插件代码得另外写
	for i, conn := range sc.connSegments {
		if i != 0 {
			continue
		}
		_, err := conn.Query(query)
		if err != nil {
			fmt.Println(err)
		}
	}
}

//之前还想着要搞几个routine出来同时start，现在看来没必要。
func (sc *CDCController) StartReplication() {

	/*
		err := sc.repConnMaster.StartReplication("slot1", 0, -1)
		if err != nil {
			fmt.Println(err)
		}
	*/

	//这里也先只跑一下7000
	for i, repConn := range sc.repConnSegments {
		if i != 0 {
			continue
		}
		err := repConn.StartReplication("slot1", 0, -1)
		if err != nil {
			fmt.Println(err)
		}
	}
}

func (sc *CDCController) DropReplicationSlot() {

}

//构造函数
//错误处理
//所有的segment用的都是一样的用户名和密码吗？
func NewServerController(opts *Options) (sc *CDCController) {
	var option ConnOption
	option.ip = "10.117.190.107" //是ip的问题吗。。。127.0.0.1不行吗。。。
	option.port = 15432
	option.databaseName = "testdb"
	option.user = "gpadmin"
	option.password = ""

	connMaster := InitOneConn(option, true)
	connMasterCommand := InitOneConn(option, true)
	repConnMaster := InitOneRepConn(option)

	//get segments' ip and port
	var connSegments []*pgx.Conn
	var repConnSegments []*pgx.ReplicationConn

	rows, err := connMaster.Query("select * from gp_segment_configuration")
	if err != nil {
		fmt.Println(err)
	}

	var content int
	for rows.Next() { //这个顺序是按我们预期的吗？
		err := rows.Scan(nil, &content, nil, nil, nil, nil, &option.port, nil, &option.ip, nil) //传空，真是厉害
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("%s, %d\n", option.ip, option.port)
		if content == -1 {
			continue
		}

		conn := InitOneConn(option, false)
		connSegments = append(connSegments, conn)

		repConn := InitOneRepConn(option)
		repConnSegments = append(repConnSegments, repConn)
	}

	sc = &CDCController{
		connMaster:        connMaster, //反正时间也够，可以看看怎么写最合适
		connMasterCommand: connMasterCommand,
		repConnMaster:     repConnMaster,
		connSegments:      connSegments,
		repConnSegments:   repConnSegments,
	}

	return
}

// 看下new和prepare，怎么把代码两边分一下。。。
func (sc *CDCController) Prepare() {

	sc.StartReplication()
	sc.CommitOneTransaction()

	snapshot_id := sc.ExportSnapshot()

	//full data id，还是得偷摸看下代码。。。脚本弄还是？
	//全部create，之后需要加个先检查是否之前都已经创建过slot的步骤
	//全部start，有点类似于分布式事务，之后要加个如果部分segment不成功，要持续轮询。
	//哦，这个地方可以go routine去写，等待多个routine结束

	snapshot_content := sc.GetSnapshotContent(snapshot_id) //先不派发，先手摇
	sc.DispatchSnapshot(snapshot_content)

	//
	//提交一个事务。。。我可否知道我是否需要提交，这个地方麻烦在于我怎么去找到一批能在各个segment上都有的键值
	//获取快照，可以跟上边用一个连接，这个连接要持续很长时间，话说怎么保活啊。。。
	//CREATE FUNCTION?这个我觉得应该搞到一个脚本里去
	//获取快照内容
	//把快照内容发到各插件

	//我在pqx库里看到了给插件传特有数据？用create传，还是有点早。。。
}

//命令行。。。
func main() {

	var options Options
	sc := NewServerController(&options)
	sc.Prepare()

	time.Sleep(time.Duration(70) * time.Second)
}
