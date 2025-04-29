package troutbox

import "context"

func (o *Outbox[T]) SendBatch(ctx context.Context, s MessageRepository) error {
	return o.sendBatch(ctx, s)
}
