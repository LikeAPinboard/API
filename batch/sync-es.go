package batch

import (
	"fmt"

	_ "github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/native"
	es "gopkg.in/olivere/elastic.v5"

	"github.com/LikeAPinboard/api/config"
	"github.com/LikeAPinboard/spec"

	"golang.org/x/net/context"
)

func createClient(c *config.Config) (*es.Client, error) {
	return es.NewClient(
		es.SetURL(c.ES.Url),
		es.SetSniff(false),
	)
}

func SyncRow(c *config.Config, resp *spec.PinResponse) {
	sLogger.Infof("Sync: %s", resp.String())

	client, err := createClient(c)
	if err != nil {
		sLogger.Error(err)
		return
	}

	_, err = client.Index().
		Index(c.ES.Index).
		Type("pins").
		Id(fmt.Sprint(resp.Id)).
		BodyJson(resp).
		Do(context.Background())
	if err != nil {
		sLogger.Error(err)
		return
	}

	sLogger.Info("Sync success")
}

func DeleteRow(c *config.Config, id int) {
	sLogger.Infof("Delete row Id: %d", id)

	client, err := createClient(c)
	if err != nil {
		sLogger.Error(err)
		return
	}

	res, err := client.Delete().
		Index(c.ES.Index).
		Type("pins").
		Id(fmt.Sprint(id)).
		Do(context.Background())
	if err != nil {
		sLogger.Error(err)
		return
	}

	if res.Found {
		sLogger.Info("Delete succeed")
	}
}
