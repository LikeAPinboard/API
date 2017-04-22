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

func SyncRow(c *config.Config, resp *spec.PinResponse) {
	sLogger.Infof("Sync: %s", resp.String())

	client, err := es.NewClient(
		es.SetURL(c.ES.Url),
		es.SetSniff(false),
	)

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
