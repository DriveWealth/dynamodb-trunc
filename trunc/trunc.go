package trunc

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/panjf2000/ants/v2"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MaxDeleteBatchSize = 25
)

type Truncator struct {
	client 				*dynamodb.Client
	pool				*ants.Pool
	delChan 			chan []*dynamodb.BatchWriteItemInput
	retrier 			*retry.Standard
	tableName			string
	pscan 				int
	limit 				int
	queue 				int
	maxretries 			int
	// ====
	keys 				[]types.KeySchemaElement
	keyNames 			[]string
	approxCount			int64
	scanCount 			*int64
	deleteCount 		*int64
	segmentsComplete 	*int32
	ctx 				context.Context
	cancelFunc 			context.CancelFunc
}

func NewTruncator(client *dynamodb.Client, pool *ants.Pool, tableName string, pscan, limit, queue, maxretries int) (*Truncator, error) {
	ch := make(chan []*dynamodb.BatchWriteItemInput, queue)
	rt := retry.NewStandard(func(o *retry.StandardOptions) {
		o.MaxAttempts = maxretries
	})
	tr := &Truncator{
		client:    client,
		pool:      pool,
		delChan:	ch,
		tableName: tableName,
		retrier: rt,
		pscan:     pscan,
		limit:     limit,
		queue:		queue,
		maxretries: maxretries,
		scanCount:	aws.Int64(int64(0)),
		deleteCount: aws.Int64(int64(0)),
		segmentsComplete: aws.Int32(int32(0)),
	}
	return tr.initialize()
}

func (t *Truncator) String() string {
	return fmt.Sprintf("Truncator: table=%s, poolcap=%d, pscan=%d, limit=%d, q=%d, retries=%d", t.tableName, t.pool.Cap(), t.pscan, t.limit, t.queue, t.maxretries)
}

func (t *Truncator) initialize() (*Truncator, error) {
	tableDef, err := t.describeTable()
	if err != nil {
		return nil, err
	}
	t.keys = tableDef.KeySchema
	t.approxCount = tableDef.ItemCount
	t.keyNames = make([]string, len(t.keys))
	for i,k := range t.keys {
		t.keyNames[i] = *k.AttributeName
	}
	return t, nil
}

func (t *Truncator) Start() {
	t.ctx, t.cancelFunc = context.WithCancel(context.TODO())
	t.consume()
	t.monitor()
	t.startPScan(t.ctx)
	log.Printf("Started Truncator: table=%s\n", t.tableName)
}

func (t *Truncator) consume() {
	go func() {
		for {
		  d :=	<- t.delChan
			t.pool.Submit(func() {
				t.executeDeletion(t.ctx, d)
			})
		}
	}()
}

func (t *Truncator) monitor() {
	go func() {
		for {
			time.Sleep(time.Second * 5)
			log.Printf("scans=%d, channel=%d, deletes=%d, goroutines=%d, poolbusy=%d\n", atomic.LoadInt64(t.scanCount), len(t.delChan), atomic.LoadInt64(t.deleteCount), runtime.NumGoroutine(), t.pool.Running())
		}
	}()
}

func (t *Truncator) describeTable() (*types.TableDescription, error) {
	descr := &dynamodb.DescribeTableInput{
		TableName: &t.tableName,
	}
	tab, err := t.client.DescribeTable(context.TODO(), descr)
	if err != nil {
		return nil, err
	}
	return tab.Table, nil
}

func (t *Truncator) startPScan(ctx context.Context) {
	lim := int32(t.limit)
	totalSegs := int32(t.pscan)
	scans := make([]*dynamodb.ScanInput, t.pscan)
	for i := 0; i < t.pscan; i++ {
		seg := int32(i)
		scans[i] = &dynamodb.ScanInput{
			TableName:                 &t.tableName,
			AttributesToGet:           t.keyNames,
			Limit:                     &lim,
			Segment:                   &seg,
			TotalSegments:             &totalSegs,
		}
	}
	for i := 0; i < t.pscan; i++ {
		x := i
		t.pool.Submit(func() {
			t.executeSegment(ctx, scans[x], 0)
		})

	}
}

func KeysToDeleteBatches(tableName string, keys []map[string]types.AttributeValue) []*dynamodb.BatchWriteItemInput {
	batches := make([]*dynamodb.BatchWriteItemInput, 0)
	currentBatch := make([]types.WriteRequest,0)
	currentBatchSize := 0
	for _,av := range keys {
		wr := types.WriteRequest{
			 DeleteRequest: &types.DeleteRequest{av},
		}
		currentBatch = append(currentBatch, wr)
		currentBatchSize++
		if currentBatchSize == MaxDeleteBatchSize {
			currentBatchSize = 0
			batches = append(batches, &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					tableName: currentBatch,
				},
			})
			currentBatch = make([]types.WriteRequest,0)
		}
	}
	if currentBatchSize > 0 {
		batches = append(batches, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				tableName: currentBatch,
			},
		})
	}
	return batches
}

func (t *Truncator) executeDeletion(ctx context.Context, batches []*dynamodb.BatchWriteItemInput) {
	size := len(batches)
	if size == 0 {
		return
	}
	var wg sync.WaitGroup
	wg.Add(size)
	for i := 0; i < size; i++ {
		x := i
		t.pool.Submit(func() {

			t.executeBatch(ctx, batches[x], 0, &wg)
		})
	}

	wg.Wait()
}

func (t *Truncator) executeBatch(ctx context.Context, batch *dynamodb.BatchWriteItemInput, retries int, wg *sync.WaitGroup) {
	itemCount := len(batch.RequestItems[t.tableName])
	output, err := t.client.BatchWriteItem(ctx, batch, func(o *dynamodb.Options) {
		o.Retryer = t.retrier
	})
	if err != nil {
		if retries <= t.maxretries {
			t.pool.Submit(func() {
				t.executeBatch(ctx, batch, retries + 1, wg)
			})
			return
		} else {
			log.Fatalf("Batch Delete Failed: %s\n", err.Error())
		}
	} else {
		unprocessed := len(output.UnprocessedItems[t.tableName])
		processed := itemCount - unprocessed
		if processed > 0 {
			atomic.AddInt64(t.deleteCount, int64(processed))
		}
		if unprocessed > 0 {
			t.executeBatch(ctx, &dynamodb.BatchWriteItemInput{
				RequestItems:                output.UnprocessedItems,
			}, retries, wg)
			return
		}
	}
	wg.Done()
}

func (t *Truncator) executeSegment(ctx context.Context, scanInput *dynamodb.ScanInput, retries int) error {
	var exKeys map[string]types.AttributeValue = nil
	for {
		output, err := t.client.Scan(ctx, scanInput, func(r *dynamodb.Options){
			r.Retryer = t.retrier
		})
		if err != nil {
			//retries = retries + 1
			//if retries == t.maxretries {
			//	return errors.New(fmt.Sprintf("Max number of retries: table=%s, max=%s, lasterror=%s", t.tableName, t.maxretries, err.Error()))
			//}
			if retries <= t.maxretries {
				t.pool.Submit(func() {
					t.executeSegment(ctx, scanInput, retries + 1)
				})
			} else {
				log.Fatalf("ExecuteSegment Failure: %s\n", err.Error())
				return err
			}
		} else {
			if output.Count > 0 {
				atomic.AddInt64(t.scanCount, int64(output.Count))
				deleteBatches := KeysToDeleteBatches(t.tableName, output.Items)
				t.delChan <- deleteBatches
			}
			exKeys = output.LastEvaluatedKey
			if exKeys == nil || len(exKeys) == 0 {
				break
			}
			scanInput.ExclusiveStartKey = exKeys
		}
	}
	return nil
}