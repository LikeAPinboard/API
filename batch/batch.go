package batch

import (
	"go.uber.org/zap"
)

var sLogger *zap.SugaredLogger

func init() {
	logger, _ := zap.NewProduction()
	sLogger = logger.Sugar()
}
