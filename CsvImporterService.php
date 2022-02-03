<?php

namespace App\Services;

use App\Customer;
use App\CsvImport;
use App\CsvImportBatch;
use App\CsvImportJob;
use League\Csv\Reader;
use League\Csv\Writer;
use App\Jobs\ProcessDot;
use Webpatser\Uuid\Uuid;
use App\Mail\DotFileImported;
use App\Mail\DotImportFailed;
use Price\SessionNonceHandler;
use App\Exceptions\CsvImporterException;
use Illuminate\Foundation\Bus\DispatchesJobs;

class CsvImporterService
{
    use DispatchesJobs, SessionNonceHandler;

    const CSV_RECORD_LIMIT = 1;
    const CSV_FILEPATH = 'app/csv_importer/';
    const COL_WILDCARD_PREFIX = '_#';

    public $price_config;
    public $file_meta;
    public $price_user;
    public $csv;
    private $transaction;
    private $col_header;
    protected $customer;
    protected $csv_import;
    protected $import_settings;
    protected $field_headers = [];
    protected $resource_list = [];
    protected $tz;
    protected $import_config;
    // Current CSV row offset
    protected $offset;
    /**
     * Definition: los_dataset => PRICE class name
     * There's no need to add mapping for los_dataset
     * that has the same name as PRICE class
     *
     * @var array
     */
    protected $dataset_resource_mapping = [
        'Borrower' => 'Borrowers',
        'Customer' => 'Customers',
        'Loan2' => 'Adjustment',
        'Property' => 'Properties',
        'Company' => 'Companies',
        'LoanHMDA' => 'LoanHmda',
    ];
    /**
     * On-the-fly data
     * Headers that has no settings in loan_import_settings table
     *
     * @var array
     */
    protected $otf_data_prefixes = [
        'ExtraData_',
        'Date_',
        'Fee_',
    ];

    public function __construct(\App\Dot\ImporterConfig $config)
    {
        $this->import_config = $config;
        $this->price_config = new \Price\Config;
        $this->price_config->setPriceDb($config->priceDb());
        $this->customer = $config->customer();
        $this->price_user = $config->priceUser();
    }

    /**
     * Get import config
     *
     * @return \App\Dot\ImporterConfig
     */
    public function importConfig()
    {
        return $this->import_config;
    }

    public function setCsvFile(Reader $csv, array $meta)
    {
        $this->file_meta = $meta;
        // Exclude row 1 / Header row from CSV
        $csv->setHeaderOffset(0);
        $this->csv = $csv;

        return $this;
    }

    public function csvFile()
    {
        return $this->csv;
    }

    /**
     * Set transaction type
     *
     * @param string $transaction
     * @return mixed
     */
    public function setTransaction(string $transaction)
    {
        $this->transaction = $transaction;

        return $this;
    }

    /**
     * Set timezone
     *
     * @param string $tz
     * @return mixed
     */
    public function setTimezone(string $tz)
    {
        $this->tz = $tz;

        return $this;
    }

    /**
     * Get CSV total row count
     *
     * @return int
     */
    public function rowCount()
    {
        return count($this->csv);
    }

    /**
     * Process CSV file
     *
     * @return void
     */
    public function process()
    {
        dispatch(
            (new ProcessDot($this, json_encode($this->csv)))
            ->onQueue(config('pclender.queue_csv_importer'))
        );
    }

    /**
     * Save CSV details to MySQL db
     *
     * @return mixed
     */
    public function saveCsvDetails()
    {
        $this->csv_import = CsvImport::create([
            'customer_id' => $this->customer->id,
            'file_path' => $this->file_meta['uploaded_file'],
            'orig_filename' => $this->file_meta['orginal_filename'],
            'total_rows' => $this->rowCount(),
            'transaction' => $this->transaction,
            'tz' => $this->tz,
        ]);

        return $this;
    }

    /**
     * Set CsvImport
     *
     * @return mixed
     */
    public function setCsvImport($csv_import)
    {
        $this->csv_import = $csv_import;

        return $this;
    }

    /**
     * Get CsvImport
     *
     * @return \App\CsvImport
     */
    public function csvImport()
    {
        return $this->csv_import;
    }

    /**
     * Check if transaction is for new loan, if not, then transaction for update
     *
     * @return boolean
     */
    public function isNewLoan()
    {
        return $this->transaction == 'add' ? true : false;
    }

    /**
     * Set the import settings
     *
     * @return mixed
     */
    public function setImportSettings()
    {
        // possible value is - isInsertFields or isUpdateFields
        // these are scope methods under \App\LoanImportSetting class
        $trans_condition = 'is'. ($this->isNewLoan() ? 'Insert' : 'Update') .'Fields';
        $customer_id = $this->customer->id;
        // sanitize headers for query
        $headers = explode(',',
            addslashes(
                implode(',', $this->csv->getHeader()
            )
        ));
        // Query the import settings and its custom settings
        $this->import_settings = \App\LoanImportSetting::with([
                'customImportSettings' => function($q) use($customer_id) {
                    return $q->where('customer_id', $customer_id); 
                }
            ])
            ->dotVersion()
            ->losFieldByHeaders($headers)
            ->{$trans_condition}()
            ->get();

        return $this;
    }

    public function getImportSettings()
    {
        return $this->import_settings;
    }

    public function setOffset($offset)
    {
        $this->offset = $offset;

        return $this;
    }

    public function getOffset()
    {
        return $this->offset;
    }

    /**
     * Get header settings
     *
     * @return \App\LoanImportSetting|null
     */
    public function getSettingsByHeader()
    {
        // possible value is - isInsertFields or isUpdateFields
        // these are scope methods under \App\LoanImportSetting class
        $trans_condition = 'is'. ($this->isNewLoan() ? 'Insert' : 'Update') .'Fields';
        $customer_id = $this->customer->id;
        $col_header = $this->getWildcardColumn() ? : $this->getColHeader();
        // Query the import settings and its custom settings
        $settings = \App\LoanImportSetting::with([
                'customImportSettings' => function($q) use($customer_id) {
                    return $q->where('customer_id', $customer_id); 
                }
            ])
            ->{$trans_condition}()
            ->where('los_field_name', $col_header)
            ->dotVersion()
            ->first();
        if (!is_null($settings)) return $settings;

        // Custom Header title
        $custom_settings = $this->customer
            ->loanImportSettings()
            ->dotVersion()
            ->where('field_name', $col_header)
            ->first();
        return $custom_settings;
    }

    /**
     * Validate the column data type
     *
     * @param string $value
     * @return void|boolean|Exception
     */
    public function validateCsv($value)
    {
        $header_settings = $this->getSettingsByHeader();
        // Ignore the header if no record found in the loan_import_settings table
        if (is_null($header_settings)) return;

        return $header_settings->validateHeaderValue($value);
    }

    /**
     * Set current column header
     *
     * @param string $header
     * @return mixed
     */
    public function colHeader($header)
    {
        $this->col_header = $header;

        return $this;
    }

    /**
     * Get current column header
     *
     * @return string
     */
    public function getColHeader()
    {
        return $this->col_header;
    }

    /**
     * Process header dataset
     *
     * @param string $value
     * @return mixed
     */
    public function setHeaderDataset($value)
    {
        // Skip blank values
        if ($value == '') return $this;
        if ($this->checkOnTheFlyData($value)) return $this;

        $header_settings = $this->getSettingsByHeader($this->getColHeader());
        if (is_null($header_settings)) {
            throw new CsvImporterException(
                CsvImporterException::HEADER_NOT_SUPPORTED,
                $this->getColHeader()
            );
        }

        // cross-reference value
        $xref_val = $header_settings->getXrefValue($value);
        $value = !is_null($xref_val) ? $xref_val : $value;

        $this->fillResourceData(
            $header_settings->los_dataset,
            $this->getDatasetResource($header_settings->los_dataset),
            $value
        );

        return $this;
    }

    /**
     * Get the equavalent of dataset resource property
     *
     * @param string $dataset
     * @return string
     */
    private function getDatasetResource(string $dataset)
    {
        // Note: Put this to database for future changes in the mapping
        $map = [
            'Borrower' => 'borrower_data',
            'Customer' => 'customer_data',
            'Property' => 'subject_property_data',
            'Loan' => 'loan_data',
            'Loan2' => 'adjustment_data',
            'LoanServicing' => 'loan_servicing_data',
            'LoanCorrespondent' => 'loan_correspondent_data',
            'LoanHMDA' => 'loan_hmda_data',
            'Company' => 'company_data',
            'LoanLicense' => 'loan_license_data',
            'InvestorFeatureCode' => 'investor_code_data',
            'LoanCorrespondentFee' => 'loan_correspondent_fee_data',
            'LoanCorrespondentAdjustment' => 'loan_correspondent_adjustment_data',
        ];

        if (isset($map[$dataset])) return $map[$dataset]; 

        throw new CsvImporterException(
            CsvImporterException::RESOURCE_PROPERTY_NOT_EXISTS,
            $this->getColHeader()
        );
    }

    /**
     * Check and fill resource data for on-the-fly headers
     *
     * @param string $value
     * @return boolean
     */
    private function checkOnTheFlyData(string $value)
    {
        // ExtraData column header
        if ($this->isExtraDataField()) {
            $this->fillResourceData(
                'ExtraData',
                'extra_data',
                $value
            );
            return true;
        }

        // DateData column header
        if ($this->isDateDataField()) {
            $this->fillResourceData(
                'AddOrUpdateDate',
                'date_data',
                $value
            );
            return true;
        }

        // DateData column header
        if ($this->isFeeDataField()) {
            $this->fillResourceData(
                'Fees',
                'fee_data',
                $value
            );
            return true;
        }

        // Virtual column header
        if ($this->isVirtualDataField()) {
            $this->fillResourceData(
                'VirtualData',
                'virtual_data',
                $value
            );
            return true;
        }

        return false;
    }

    /**
     * Get valid prefixes
     *
     * @return array
     */
    public function validPrefixes()
    {
        return [
            'Borrower_',
            'Subject_Property_',
            'HMDA_Data_',
            'Virtual_',
            'ExtraData_',
            'Date_',
            'Company_',
            'Corresp_',
            'ULDD_',
            'Commit_',
        ];
    }

    /**
     * Get the header prefix
     *
     * @return string
     */
    public function getHeaderPrefix()
    {
        // get the prefix based on the valid defined prefix
        $prefix = '';
        for ($i=0; $i < count($this->validPrefixes()); $i++) { 
            if (strpos($this->getColHeader(), $this->validPrefixes()[$i]) === 0) {
                $prefix = $this->validPrefixes()[$i];
            }
        }

        return $prefix;
    }

    /**
     * Parse header to get the PRICE field
     *
     * @return string
     */
    public function parseHeader()
    {
        // check if the prefix is valid, then replace it with blank
        $prefix = $this->getHeaderPrefix();
        if (in_array($prefix, $this->validPrefixes())) {
            return str_replace($prefix, '', $this->getColHeader());
        }

        // default value is the actual column header
        return $this->getColHeader();
    }

    /**
     * Get header PRICE field.
     *
     * @return string
     */
    public function getHeaderField()
    {
        $header_field = $this->headerFieldMapping($this->parseHeader());
        // check if the last character is not numeric
        // or if the field is excempted to have numeric suffix
        if (
            !is_numeric(substr($this->parseHeader(), -1)) ||
            $this->fieldWithIndexException($header_field)
        ) {
            return $header_field;
        }

        // For headers that does not excempted to have index
        // Remove header index to get the actual PRICE field name
        $header_arr = explode('_', $this->parseHeader());
        array_pop($header_arr);
        return $this->headerFieldMapping(implode('_', $header_arr));
    }

    /**
     * Check if header has ExtraData_ prefix
     *
     * @return boolean
     */
    public function isExtraDataField()
    {
        return strpos($this->getColHeader(), 'ExtraData_') === 0;
    }

    /**
     * Check if header has Date_ prefix
     *
     * @return boolean
     */
    public function isDateDataField()
    {
        return strpos($this->getColHeader(), 'Date_') === 0;
    }

    /**
     * Check if header has Fee_ prefix
     *
     * @return boolean
     */
    public function isFeeDataField()
    {
        return strpos($this->getColHeader(), 'Fee_') === 0;
    }

    /**
     * Check if header has Virtual_ prefix
     *
     * @return boolean
     */
    public function isVirtualDataField()
    {
        return strpos($this->getColHeader(), 'Virtual_') === 0;
    }

    /**
     * Field mapping for header that doesn't have equivalent PRICE field
     *
     * @param string $field
     * @return string
     */
    public function headerFieldMapping(string $field)
    {
        // csv header => price field
        $fields = [
            'SSN' => 'Social_Security_Number',
            'Home_Phone' => 'Voice',
            'Work_Phone' => 'Work_Number',
            'Email' => 'email_address',
            'Classification' => 'Property_Classification',
            'Credit_Score_Dt' => 'Credit_Score_Date',
            'Property_Type' => 'Loan_Commit_Property_Type',
            'Adj_Total_Dollar_2' => 'Loan_Commit_Adj_Total_Dollar_2',
            'Points_Dollar' => 'Loan_Commit_Points_Dollar',
            'SRP_Dollar' => 'Loan_Commit_SRP_Dollar',
            'Base_Price_Dollar' => 'Loan_Commit_Base_Price_Dollar',
            'SRP' => 'Commit_SRP',
            'Type' => 'Property_Type',
        ];

        return isset($fields[$field]) ?
            $fields[$field] :
            $field;
    }

    /**
     * Fill and prepare resource/dataset data based
     *
     * @param string $dataset
     * @param string $resource_property
     * @param string $value
     * @return mixed|Exception
     */
    public function fillResourceData($dataset, $resource_property, $value)
    {
        // Don't accept empty values
        if (!$value) return $this;

        $namespace = $dataset == 'AddOrUpdateDate' ?
            '\\Price\\Misc\\' : '\\Price\\Resources\\';
        $class_name = isset($this->dataset_resource_mapping[$dataset]) ?
            $namespace. $this->dataset_resource_mapping[$dataset] :
            $namespace. $dataset;
        if (!class_exists($class_name)) {
            throw new CsvImporterException(
                CsvImporterException::RESOURCE_CLASS_NOT_FOUND,
                $this->getColHeader()
            );
        }
        if (!$resource_property) {
            throw new CsvImporterException(
                CsvImporterException::RESOURCE_PROPERTY_NOT_EXISTS,
                $this->getColHeader()
            );
        }
        if (!isset($this->{$resource_property})) {
            $this->{$resource_property} = [];
        }

        // use the number in the header as index, if non-numeric use 0
        $index = 0;
        if (!$this->fieldWithIndexException($this->getHeaderField())) {
            $index = (int) substr($this->getColHeader(), -1) - 1;
            $index = $index > -1 ? $index : 0;
        }

        // $index is appended to the field name to have a unique field name
        // for multiple entry like in borrower's first_name, last_name, etc..
        $this->setFieldHeader(
            $dataset.$this->getHeaderField().$index,
            $this->getColHeader()
        );

        if ($this->getWildcardColumn() !== false) {
            // assumed that the dataset have only 1 recordset and does have
            // wildcard fields or group columns
            $this->{$resource_property}[0]['wildcards'][$index][$this->getHeaderField()] = $value;
        }
        else {
            $this->{$resource_property}[$index][$this->getHeaderField()] = $value;
        }
        $this->resource_list[] = $resource_property;

        return $this;
    }

    /**
     * Check if column header is excempted from parsing index
     *
     * @param string $header
     * @return bool
     */
    public function fieldWithIndexException($header)
    {
        $except = [
            'Loan_Commit_Adj_Total_Dollar_2',
            'CorrespondentLockExtensionFees1',
            'CorrespondentLockExtensionFees2',
            'CorrespondentLockExtensionFees3',
        ];

        return in_array($header, $except);
    }

    /**
     * Get resource property
     *
     * @param string $property_name
     * @return array|void
     */
    public function getResourceProperty($property_name)
    {
        if (!property_exists($this, $property_name)) {
            throw new CsvImporterException(
                CsvImporterException::RESOURCE_PROPERTY_NOT_FOUND,
                $this->getColHeader()
            );
        }

        return $this->{$property_name};
    }

    /**
     * Log import
     *
     * @param array $log
     * @return void
     */
    public function logImport(array $log)
    {
        $log['loan_id'] = $this->price_config->getLoanId();
        $this->csvImport()->logs()->create($log);
    }

    /**
     * Set customer data based on the current logged in user
     *
     * @return App\Customer
     */
    public function getCustomer()
    {
        return $this->customer;
    }

    /**
     * Generates summary report to CSV file
     *
     * @return array
     */
    public function generateSummaryReport()
    {
        try {
            $summary_file = null;
            $logs = $this->csvImport()
                ->logs()
                ->isFailed()
                ->get();
            // flatten the log records to be CSV ready format
            if ($log_count = $logs->count()) {
                $arr_logs = $logs->map(function($value){
                    return array_values($value->toArray());
                })->toArray();
                $summary_headers = [ 'Row #', 'Loan ID', 'Status', 'Description', ];
                $filename = substr(Uuid::generate()->string, 0, 8) .'.csv';
                $summary_file = storage_path(self::CSV_FILEPATH.$filename);
                touch($summary_file);
                $writer = Writer::createFromPath($summary_file, 'w+');
                $writer->insertAll(array_merge([$summary_headers], $arr_logs));
            }

            return [
                'log_file' => $summary_file,
                'log_count' => $log_count,
            ];
        } catch (\Exception $e) {
            \Log::error('[DOT] Unable to generate summary report. Error:'.$e->getFile().'@'. $e->getLine() . ' - ' .$e->getMessage());

            return [
                'log_file' => '',
                'log_count' => 0,
            ];
        }
    }

    /**
     * Clean up CSV file and data
     *
     * @return void
     */
    public function cleanUp()
    {
        try {
            // clear session
            $this->clearSession(\Session::getId());
            // delete CSV file
            \Storage::delete($this->csvImport()->file_path);
            \Log::info("[DOT] CSV file deleted.");
        } catch (\Exception $e) {
            \Log::error("[DOT] Unable to delete csv file. Error: ".
                $e->getMessage());
        }
    }

    /**
     * Check if the process is in the end of file
     *
     * @return boolean
     */
    public function isEndOfFIle()
    {
        return $this->csvImport()->total_rows ==
            $this->csvImport()->totalJobProcessed();
    }

    /**
     * Set field header mapping - reverse mapping for logging purposes
     *
     * @param string $field  Actual PRICE field name
     * @param string $header  CSV column header
     * @return mixed
     */
    public function setFieldHeader($field, $header)
    {
        $this->field_headers[$field] = $header;

        return $this;
    }

    /**
     * Get the original headers used in a PRICE call
     *
     * @param array $fields  Actual PRICE fields
     * @param string $dataset
     * @param int $key_suffix  Key suffix is used to make the array key unique
     * @return string
     */
    public function getOriginalHeaders(
        array $fields,
        string $dataset,
        int $key_suffix = 0
    )
    {
        if (!count($fields)) return '';

        // Create a repeated string of $key_suffix base on field count to be
        // use in array_map(). Actual fields doesn't have suffix, e.g. 
        // First_Name, to make it unique we add suffix depends on the number of
        // entry in the CSV, e.g. First_Name0, First_Name1, and so on.
        $suffixes = explode(',', 
            substr(str_repeat($key_suffix.',', count($fields)), 0, -1)
        );
        $fields_with_suffix = array_map(function($key, $suffix) use($dataset){
            return $dataset.$key.$suffix;
        }, $fields, $suffixes);
        // search original headers by field name with suffix
        $original_headers = array_intersect_key(
            $this->field_headers, array_flip($fields_with_suffix)
        );

        return implode(',', array_values($original_headers));
    }

    /**
     * Set property dynamically
     *
     * @param string $name
     * @param $value
     */
    public function __set($name, $value)
    {
        $this->{$name} = $value;
    }

    /**
     * Set property dynamically
     *
     * @param string $name
     */
    public function __get($name)
    {
        return $this->{$name};
    }

    /**
     * Check if a dataset exists via resource property name
     *
     * @param string $property_name
     * @return boolean
     */
    public function isResourceValid($property_name)
    {
        return in_array($property_name, $this->resource_list);
    }

    /**
     * Get column with wildcard prefix
     *
     * @return string|bool
     */
    public function getWildcardColumn()
    {
        $columns_with_wildcard = [
            'Commit_Price_Adjustment',
            'Commit_Price_Adjustment_Description',
            'Commit_Price_Adjustment_Value',
            'Corresp_FeeAmount',
            'Corresp_FeeDescription',
            'Corresp_FeeID',
            'Corresp_PriceAdjustmentEnabled',
            'Corresp_PriceAdjustmentDescription',
            'Corresp_PriceAdjustmentAmount',
            'Corresp_PriceAdjustmentID',
        ];
        $col_header = preg_replace( '/_[0-9]+$/', '', $this->getColHeader());
        if (in_array($col_header, $columns_with_wildcard)) {
            return $col_header.self::COL_WILDCARD_PREFIX;
        }

        return false;
    }

    /**
     * Unqueue/delete job in jobs table
     *
     * @return void
     */
    public function unqueueJobs()
    {
        $csv_jobs = \App\CsvImportJob::unstartedJobs($this->csvImport()->id)
            ->get();
        if ($csv_jobs->count()) {
            foreach ($csv_jobs as $job) {
                // tag job as processed
                $job->where('job_id', $job->job_id)
                    ->update(['is_processed' => 2]);
                // remove the job in queue
                $job->queueing->delete();

                $this->logImport([
                    'row_num' => $job->row_num,
                    'col_name' => 'Cancelled',
                    'col_headers' => '',
                    'status' => 3,
                    'description' => 'An error has been encountered in previous processing.',
                ]);
            }
            \Log::info('[DOT] Unqueued jobs for customer: '.
                $this->price_user['name'].' - '.$this->price_user['company']. 
                '. File: ' .$this->csvImport()->orig_filename);
        }
    }

    /**
     * Send email notification to customer
     *
     * @return void
     */
    public function sendEmailNotification()
    {
        // send email to customer with attached import summary in CSV
        $employee = $this->price_user;
        if ($employee['email']) {
            try {
                $summary_report = $this->generateSummaryReport();
                $email = (new DotFileImported(
                    $employee,
                    $summary_report
                ))->onQueue(config('pclender.queue_listen_on'));
                \Mail::to($employee['email'])
                    ->queue($email);
            } catch (\Exception $e) {
                \Log::error('[DOT] Unable to send email for '.
                    $employee['name'] .'. Error: '. $e->getMessage());
            }
        }
        else {
            \Log::notice('[DOT] Customer '. $employee['name'].
                ' does not have email.');
        }
    }

    /**
     * Email notification to customer and support for failed job attempt
     *
     * @return void
     */
    public function sendFailureNotification()
    {
        // send email to customer with attached import summary in CSV
        $employee = $this->price_user;
        if ($employee['email']) {
            try {
                $email = (new DotImportFailed($employee))
                    ->onQueue(config('pclender.queue_listen_on'));
                \Mail::to($employee['email'])
                    ->cc(config('pclender.support_email'))
                    ->queue($email);
            } catch (\Exception $e) {
                \Log::error('[DOT] Unable to send email for '.
                    $employee['email'] .'. Error: '. $e->getMessage());
            }
        }
        else {
            \Log::notice('[DOT][FAILED] Customer email and support 
                email are empty.');
        }
    }

    public function retry()
    {
        // TODO: Add table to save Dataset that has been succefully run in PRICE
        // so it can be use in the retry if the process stops somewhere in the middle.
        // On retry, just skip the dataset if already tagged as done.
        // schema suggestion: csv_import_id, row_num, dataset
        $import_job = CsvImportJob::where('csv_import_id', $this->csvImport()->id)
            ->where('row_num', $this->importConfig()->currentRow())
            ->first();
        if (!is_null($import_job)) {
            $tries = $import_job->tries + 1;
            if ($tries < CsvImportJob::MAX_TRIES) {
                CsvImportJob::where('csv_import_id', $this->csvImport()->id)
                ->where('row_num', $this->importConfig()->currentRow())
                ->update([
                    'is_processed' => CsvImportJob::STATUS_PENDING,
                    'tries' => $tries,
                ]);
                \Log::info('[JOB][DOT] Retrying row #'. $import_job->row_num . '. Tries: '. $tries);
            }
            else {
                // update specific row to max_tries
                CsvImportJob::where('csv_import_id', $this->csvImport()->id)
                ->where('row_num', $this->importConfig()->currentRow())
                ->update([
                    'tries' => $tries,
                ]);
                // set all jobs status to is_processed
                CsvImportJob::where('csv_import_id', $this->csvImport()->id)
                ->update([
                    'is_processed' => CsvImportJob::STATUS_IS_PROCESSED,
                ]);

                $this->csvImport()->status = 3;
                $this->csvImport()->save();

                // clear queueing for the current import
                $this->unqueueJobs();

                CsvImportBatch::where('id', $import_job->batch_id)
                ->update([
                    'is_done' => CsvImportBatch::STATUS_DONE,
                ]);

                $this->sendFailureNotification();
                $this->cleanUp();
            }
        }
    }

    /**
     * Total number of processed rows
     *
     * @return int
     */
    public function totalProcessed()
    {
        return $this->csvImport()
            ->jobs()
            ->where('is_processed', CsvImportJob::STATUS_IS_PROCESSED)
            ->count();
    }
}
