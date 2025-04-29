/*
This package provides an implementation of the TransactionalMessageRepository interface for a PostgreSQL database.
It also includes partitioning support for the outbox table.

# Usage example

		// Create the PostgreSQL adapter.
		psqlRepo := psql.NewPostgresMessageRepository(db,
	    	psql.WithTableName("outbox_messages"),
	    	psql.WithFuturePartitions(3),
	    	psql.WithPastPartitions(1),
	    	psql.WithJobPeriod(1*time.Hour),
	    )

		// Migrate the database: create tables and indexes, remove old partitions.
	    if err := psqlRepo.Migrate(ctx); err != nil {
	        log.Fatalf("Failed to migrate database: %v", err)
	    }

		// Run partioning job: create future partitions and remove old ones.
		go func() {
			if err := psqlRepo.RunPartitioningJob(ctx); err != nil {
				log.Fatalf("Failed to run partitioning job: %v", err)
			}
		}()
*/
package psql
