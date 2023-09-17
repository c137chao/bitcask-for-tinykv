PROJECT="bitcask"

GO             := GO111MODULE=on go
GOBUILD        := $(GO) build $(BUILD_FLAG) -tags codes
GOTEST         := $(GO) test -v --count=1 --parallel=1 -p=1
CLEAN          := rm -rf /tmp/bitcask-go*
TEST_LDFLAGS   := ""

redis:


https:
	$(GO) run ./http/main.go

clean:
	$(CLEAN)