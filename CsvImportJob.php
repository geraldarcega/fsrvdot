<?php

namespace App;

use Illuminate\Database\Eloquent\Model;

class CsvImportJob extends Model
{
    const MAX_TRIES = 3;
    const STATUS_PENDING = 0;
    const STATUS_IN_PROGRESS = 1;
    const STATUS_IS_PROCESSED = 2;

    public $incrementing = false;
    public $timestamps = false;
    protected $primaryKey = null;
    protected $fillable = [
        'row_num',
        'job_id',
        'payload',
        'batch_id',
    ];

    public function csvImport()
    {
        return $this->belongsTo('App\CsvImport');
    }

    public function batch()
    {
        return $this->belongsTo('App\CsvImportBatch', 'batch_id');
    }
    
    /**
     * Get the job associated with the CsvImportJob
     *
     * @return \Illuminate\Database\Eloquent\Relations\HasOne
     */
    public function queueing()
    {
        return $this->belongsTo('App\Job', 'job_id');
    }

    public function scopeUnstartedJobs($query, $csv_import_id)
    {
        return $query->select('csv_import_jobs.*', 'jobs.id', 'jobs.attempts')
            ->join('jobs', 'id', '=', 'job_id')
            ->where('attempts', 0) // not started yet
            ->where('csv_import_id', $csv_import_id);
    }

    /**
     * Total number of jobs processed in batch
     *
     * @param int $batch_id
     * @return int
     */
    static public function numberOfProcessedInBatch(int $batch_id)
    {
        return self::where('batch_id', $batch_id)
            ->where('is_processed', '<>', 0)
            ->count();
    }
}
