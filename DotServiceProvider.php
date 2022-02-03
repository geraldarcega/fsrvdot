<?php

namespace App\Providers;

use App\CsvImportBatch;
use App\CsvImportJob;
use Illuminate\Support\Facades\Queue;
use Illuminate\Support\ServiceProvider;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\Events\JobProcessing;

class DotServiceProvider extends ServiceProvider
{
    /**
     * Bootstrap services.
     *
     * @return void
     */
    public function boot()
    {
        Queue::before(function (JobProcessing $event) {
            if ($event->job->resolveName() == 'App\Jobs\ImportCsv') {
                try {
                    $csv_job = new CsvImportJob;
                    $csv_job->where('job_id', $event->job->getJobId())
                    ->update([
                        'is_processed' => 1,
                    ]);
                }
                catch(\Exception $e) {
                    \Log::error("[DOT][QUEUE] Unable to get job details. " . $e->getMessage());
                }
            }
        });

        Queue::after(function (JobProcessed $event) {
            if ($event->job->resolveName() == 'App\Jobs\ImportCsv') {
                try {
                    $import_csv_class = unserialize(
                        $event->job->payload()['data']['command']
                    );
                    $csv_service = $import_csv_class->csv_service;
                    $job = CsvImportJob::where('job_id', $event->job->getJobId())->first();
                    CsvImportJob::where('job_id', $event->job->getJobId())
                    ->update([
                        'tries' => $job->tries + 1,
                        'is_processed' => 2,
                    ]);
                    $job = CsvImportJob::where('job_id', $event->job->getJobId())->first();
                    $batch = CsvImportBatch::find($job->batch_id);
                    $batch->rows_processed = $csv_service->totalProcessed();

                    if ($batch->rows_per_batch == CsvImportJob::numberOfProcessedInBatch($job->batch_id)) {
                        $batch_seq = [
                            1 => 10,
                            10 => 30,
                            30 => 30,
                        ];
                        $batch->rows_per_batch = isset($batch_seq[$batch->rows_per_batch]) ? 
                            $batch_seq[$batch->rows_per_batch] : 
                            30;
                    }
                    $batch->save();

                    if ($csv_service->isEndOfFIle()) {
                        $csv_service->sendEmailNotification();
                        $logs = $csv_service->csvImport()
                            ->logs()
                            ->hasFailure()
                            ->first();
                        $status = 1;
                        if (!is_null($logs) && $logs->has_failure) {
                            $status = 2;
                        }
                        // Set status as done
                        $csv_service->csvImport()->status = $status;
                        $csv_service->csvImport()->save();

                        $batch->is_done = CsvImportBatch::STATUS_DONE;
                        $batch->save();

                        $csv_service->cleanUp();
                    }
                }
                catch(\Exception $e) {
                    \Log::error("[DOT][QUEUE] Unable to get job details. " . $e->getMessage());
                }
            }
        });
    }

    /**
     * Register services.
     *
     * @return void
     */
    public function register()
    {
        //
    }
}
