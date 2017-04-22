package config

import (
	"fmt"

	"github.com/BurntSushi/toml"
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/native"
)

type Config struct {
	Host  string      `toml:"host"`
	Port  int         `toml:"port"`
	MySQL MySQLConfig `toml:"mysql"`
	ES    ESConfig    `toml:"elasticsearch"`
}

type MySQLConfig struct {
	Host     string `toml:"host"`
	Port     int    `toml:"port"`
	User     string `toml:"user"`
	Password string `toml:"password"`
	DBName   string `toml:"dbname"`
}

func (m MySQLConfig) Connect() (mysql.Conn, error) {
	db := mysql.New(
		"tcp",
		"",
		fmt.Sprintf("%s:%d", m.Host, m.Port),
		m.User,
		m.Password,
		m.DBName,
	)

	return db, db.Connect()
}

type ESConfig struct {
	Url   string `toml:"url"`
	Index string `toml:"index"`
}

func Init(filename string) (*Config, error) {
	if filename == "" {
		filename = "/etc/likeapinboard.conf"
	}

	conf := &Config{}
	if _, err := toml.DecodeFile(filename, conf); err != nil {
		return nil, err
	}

	return conf, nil
}
