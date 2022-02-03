<?php

namespace App;

use App\CsvImportJob;
use Illuminate\Database\Eloquent\Model;

class CsvImportBatch extends Model
{
    const BATCH_INCREMENT = 10;
    const MAX_BATCH_INCREMENT = 30;
    const STATUS_ONGOING = 0;
    const STATUS_DONE = 1;

    protected $fillable = [
        'rows_per_batch',
    ];

    public function importJobs()
    {
        return $this->hasMany('App\CsvImportJob', 'batch_id');
    }

    /**
     * Initialize batch record
     *
     * @return int
     */
    public function initBatch()
    {
        return $this->create([
            'rows_per_batch' => 1,
        ]);
    }

    /**
     * Increase number of rows to be process in the current batch
     *
     * @return void
     */
    public function increaseRowPerBatch()
    {
        // increments is 1, BATCH_INCREMENT ...MAX_BATCH_INCREMENT
        $increment = $this->rows_per_batch < self::BATCH_INCREMENT ?
            self::BATCH_INCREMENT :
            self::MAX_BATCH_INCREMENT;
        if ($increment < self::MAX_BATCH_INCREMENT) {
            return $this->update([
                'rows_per_batch' => $increment,
            ]);
        }
    }

    public function scopeIsDone($query)
    {
        return $query->where('is_done', self::STATUS_DONE);
    }

    public function scopeIsNotDone($query)
    {
        return $query->where('is_done', self::STATUS_ONGOING);
    }

    public function pendingBatches()
    {
        return $this->isNotDone()->get();
    }

    public function batchJobs(int $offset)
    {
        return $this->importJobs()
            ->where('tries', '<', CsvImportJob::MAX_TRIES)
            ->where('is_processed', '0')
            ->orderBy('row_num')
            ->limit($offset)
            ->get();
    }
}
