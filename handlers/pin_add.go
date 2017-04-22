package handlers

import (
	"fmt"
	"time"

	"github.com/LikeAPinboard/api/batch"
	"github.com/LikeAPinboard/api/config"
	"github.com/LikeAPinboard/spec"
	"github.com/ziutek/mymysql/mysql"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type addPinServer struct {
	conf *config.Config
	serviceInterface
}

func init() {
	addService(&addPinServer{})
}

func (s *addPinServer) Name() string {
	return "AddPinServer"
}

func (s *addPinServer) Register(server *grpc.Server, config *config.Config) error {
	s.conf = config
	spec.RegisterAddPinServer(server, s)
	return nil
}

func (s *addPinServer) Execute(ctx context.Context, request *spec.AddRequest) (*spec.PinResponse, error) {
	fmt.Println("add pin", request.String())
	db, err := s.conf.MySQL.Connect()
	if err != nil {
		fmt.Println("db connect failed")
		return nil, err
	}
	fmt.Println("db connected")
	now := time.Now()
	tr, err := db.Begin()
	if err != nil {
		fmt.Println("transaction start failed")
		return nil, err
	}
	fmt.Println("transaction started")
	if err != nil {
		fmt.Println("prepare failed")
		return nil, err
	}
	fmt.Println("prepare success")
	createPin, _ := db.Prepare("INSERT INTO pins VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		return nil, err
	}
	meta, err := tr.Do(createPin).Run(
		1,
		request.GetTitle(),
		request.GetDescription(),
		request.GetPhrase(),
		request.GetUrl(),
		now.Unix(),
		now.Format("2006-01-02 15:04:05"),
		now.Format("2006-01-02 15:04:05"),
	)
	if err != nil {
		fmt.Println("insert pin failed")
		tr.Rollback()
		return nil, err
	}
	fmt.Println("insert pin success")

	tagIds := make([]int, 0)
	createTag, err := db.Prepare("INSERT INTO tags VALUES (NULL, ?, ?, ?)")
	if err != nil {
		return nil, err
	}
	ti := tr.Do(createTag)
	for _, tag := range request.GetTags() {
		if id, exists := s.findByTagName(db, tag); exists {
			tagIds = append(tagIds, id)
			continue
		}
		meta, err := ti.Run(
			tag,
			now.Format("2006-01-02 15:04:05"),
			now.Format("2006-01-02 15:04:05"),
		)
		if err != nil {
			fmt.Println("insert tag failed")
			tr.Rollback()
			return nil, err
		}
		fmt.Println("insert tag success")
		tagIds = append(tagIds, int(meta.InsertId()))
	}

	relPinTags, err := db.Prepare("INSERT INTO rel_pin_tags VALUES (?, ?, ?, ?)")
	if err != nil {
		return nil, err
	}

	pinId := meta.InsertId()
	ti = tr.Do(relPinTags)
	for _, tagId := range tagIds {
		_, err := ti.Run(
			pinId,
			tagId,
			now.Format("2006-01-02 15:04:05"),
			now.Format("2006-01-02 15:04:05"),
		)
		if err != nil {
			fmt.Println("rel pin tag failed")
			tr.Rollback()
			return nil, err
		}
		fmt.Println("rel pin tag success")
	}

	tr.Commit()

	resp := &spec.PinResponse{
		Id:          int32(pinId),
		UserId:      request.GetUserId(),
		Title:       request.GetTitle(),
		Url:         request.GetUrl(),
		Timestamp:   request.GetTimestamp(),
		Description: request.GetDescription(),
		Tags:        request.GetTags(),
	}

	go batch.SyncRow(s.conf, resp)
	return resp, nil
}

func (s *addPinServer) findByTagName(db mysql.Conn, tagName string) (tagId int, exists bool) {
	row, _, err := db.QueryFirst("SELECT id FROM tags WHERE name = ?", tagName)
	if err != nil {
		return
	}
	return row.Int(0), true
}
