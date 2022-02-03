<?php

namespace App\Jobs;

use Webpatser\Uuid\Uuid;
use Illuminate\Bus\Queueable;
use App\Services\CsvImporterService;
use Illuminate\Queue\SerializesModels;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;

class ImportCsv implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    const CSV_RECORD_OFFSET = 5000;
    const CSV_STATUS_DONE = 1;
    const CSV_STATUS_DONE_WITH_FAILURE = 2;
    const CSV_STATUS_DONE_FAILED= 3;
    const IMPORT_STATUS_NOTICE = 2;
    const IMPORT_STATUS_SUCCESS = 1;
    const IMPORT_STATUS_FAILED = 0;

    public $csv_service;
    public $records;
    private $row_num;
    private $loan_resource;
    private $borrower_resource;
    private $process_string;

    /**
     * Create a new job instance.
     *
     * @return void
     */
    public function __construct(CsvImporterService $csv_service, $records)
    {
        $this->csv_service = $csv_service;
        $this->records = json_decode($records, true);
    }

    /**
     * Handles job failure
     *
     * @param $e
     * @return void
     */
    public function failed($e)
    {
        \Log::error('[JOB][DOT] Error encountered - '. $e->getMessage() . '. Retrying if possible.');

        $this->csv_service->retry();
    }

    /**
     * Execute the job.
     *
     * @return void
     */
    public function handle()
    {
        // TODO: Add method to PRICE service to ping PRICE connection
        // current CSV row number
        $row_num = $this->csv_service->getOffset() + 2;
        // stop processing other rows when a job failed
        if (
            $this->csv_service->csvImport()->status == 
            self::CSV_STATUS_DONE_FAILED
        ) {
            return;
        }

        $this->row_num = $row_num;

        // extract row values
        $values = array_values($this->records);
        // extract row headers to validate and check its settings
        foreach (array_keys($this->records) as $index => $header) {
            try {
                $this->csv_service->colHeader($header);
                if ($this->csv_service->csvImport()->transaction != 'update') {
                    // add loans
                    if ($header != 'Loan_Number_ID') {
                        $this->csv_service->validateCsv($values[$index]);
                        $this->csv_service->setHeaderDataset($values[$index]);
                    }
                }
                else {
                    $this->csv_service->validateCsv($values[$index]);
                    $this->csv_service->setHeaderDataset($values[$index]);
                }
            } catch (\Exception $e) {
                $this->csv_service->logImport([
                    'row_num' => $row_num,
                    'col_name' => 'Unsupported',
                    'col_headers' => $this->csv_service->getColHeader(),
                    'status' => self::IMPORT_STATUS_FAILED,
                    'description' => $e->getMessage(),
                ]);
            }
        }

        // Check if row failed in validation phase
        if (!$this->checkIfRowFailed()) {
            $this->setLoanId($this->records);
            $this->runProcess();
        }
    }

    /**
     * Run all process
     *
     * @return void
     */
    private function runProcess()
    {
        if ($this->csv_service->isResourceValid('loan_data'))
            $this->processLoanData();

        if ($this->csv_service->isResourceValid('borrower_data'))
            $this->processBorrowerInfo();

        if ($this->csv_service->isResourceValid('customer_data'))
            $this->processCustomerData();

        if ($this->csv_service->isResourceValid('subject_property_data'))
            $this->processSubjectProperty();

        if ($this->csv_service->isResourceValid('fee_data'))
            $this->processFeeData();

        if ($this->csv_service->isResourceValid('adjustment_data'))
            $this->processLoan2Data();

        if ($this->csv_service->isResourceValid('loan_hmda_data'))
            $this->processLoanHmdaData();

        if ($this->csv_service->isResourceValid('loan_servicing_data'))
            $this->processLoanServicingData();

        if ($this->csv_service->isResourceValid('loan_correspondent_data'))
            $this->processLoanCorrespondentData();

        if ($this->csv_service->isResourceValid('loan_license_data'))
            $this->processLoanLicense();

        if ($this->csv_service->isResourceValid('company_data'))
            $this->processCompanyData();

        if ($this->csv_service->isResourceValid('virtual_data'))
            $this->processVirtualData();

        if ($this->csv_service->isResourceValid('extra_data'))
            $this->processExtraData();

        if ($this->csv_service->isResourceValid('date_data'))
            $this->processDateData();

        if ($this->csv_service->isResourceValid('investor_code_data'))
            $this->processInvestorFeatureCode();

        if ($this->csv_service->isResourceValid('loan_correspondent_fee_data'))
            $this->processLoanCorrespondentFee();

        if ($this->csv_service->isResourceValid('loan_correspondent_adjustment_data'))
            $this->processLoanCorrespondentAdjustment();
    }

    /**
     * Determine loan ID
     *
     * @param $record
     * @return integer
     */
    private function setLoanId($record)
    {
        if ($this->csv_service->isNewLoan()) {
            // resets the Loan ID query string
            $this->csv_service->price_config->setLoanId(null);
            $channel = isset($record['Channel']) ? $record['Channel'] : '';
            // If Broker_ID exists use it as passedID
            $passed_id = isset($record['Broker_ID']) && $record['Broker_ID'] != '' ?
                $record['Broker_ID'] : (isset($record['Loan_Officer_ID']) ?
                $record['Loan_Officer_ID'] : '0');
            // add transaction
            $loan = new \Price\Resources\Loan($this->csv_service->price_config);
            $loan_id = $loan->setQueryParams([
                'Channel'   => $channel,
                'PassedID'  => $passed_id,
            ])->create();
            if (!$loan_id) {
                throw new \Exception("Unable to create loan");
            }
        }
        else {
            // update trasaction
            $loan_id = (int) $record['Loan_Number_ID'];
        }
        // check if loan_id is valid - numeric and not 0
        if (!is_numeric($loan_id) || !$loan_id) {
            $this->csv_service->logImport([
                'row_num' => $this->row_num,
                'col_name' => 'Loan',
                'col_headers' => 'N/A',
                'status' => self::IMPORT_STATUS_FAILED,
                'description' => $this->csv_service->isNewLoan() ? 'Unable to create loan' : 'Loan ID is empty',
                'processing_time' => 0,
            ]);
            throw new \Exception("Unable to create loan");
        }

        $this->csv_service->price_config->setLoanId($loan_id);
        // If $loan is set, log the response time from ADD_A_LOAN
        if (isset($loan)) {
            $this->csv_service->logImport([
                'row_num' => $this->row_num,
                'col_name' => 'Loan',
                'col_headers' => 'N/A',
                'status' => self::IMPORT_STATUS_SUCCESS,
                'description' => 'Loan created successfully',
                'processing_time' => $loan->getResponseTime(),
            ]);
        }
        // reinitialize Loan Resource class with $loan_id
        $this->loan_resource = new \Price\Resources\Loan($this->csv_service->price_config);
        $this->process_string = new \Price\Misc\ProcessString($this->csv_service->price_config);

        return $this;
    }

    private function checkEmpty($data)
    {
        if (!empty($data)) return false;

        return true;
    }

    private function checkIfRowFailed()
    {
        return $this->csv_service
            ->csvImport()
            ->logs()
            ->isFailed()
            ->whereRowNum($this->row_num)
            ->count();
    }

    /**
     * Process borrower information data
     *
     * @return void
     */
    private function processBorrowerInfo()
    {
        $this->borrower_resource = new \Price\Resources\Borrowers(
            $this->csv_service->price_config
        );
        $borrowers = $this->borrower_resource->get();
        $response_time = $this->borrower_resource->getResponseTime();
        $data = $this->csv_service->getResourceProperty('borrower_data');
        if (!$this->checkEmpty($borrowers)) {
            foreach ($data as $index => $borrower_data) {
                $orig_headers = $this->csv_service->getOriginalHeaders(
                    array_keys($borrower_data),
                    'Borrower',
                    $index
                );
                if (isset($borrowers[$index]) && !empty($borrower_data)) {
                    $update = $this->borrower_resource
                        ->setResourceId($borrowers[$index]->PersonID)
                        ->update($borrower_data);
                    if ($update->Successful) {
                        $this->csv_service->logImport([
                            'col_name' => 'Borrower',
                            'col_headers' => $orig_headers,
                            'row_num' => $this->row_num,
                            'status' => self::IMPORT_STATUS_SUCCESS,
                            'description' => 'Borrower Info Success',
                            'processing_time' => $response_time + $update->Stats->MethodTime,
                        ]);
                    }
                    else {
                        $this->csv_service->logImport([
                            'col_name' => 'Borrower',
                            'col_headers' => $orig_headers,
                            'row_num' => $this->row_num,
                            'status' => self::IMPORT_STATUS_FAILED,
                            'description' => 'Borrower #'.($index + 1) .
                            ' - ['. $update->ErrorCode .']'.$update->ErrorMessage,
                            'processing_time' => $response_time + $update->Stats->MethodTime,
                        ]);
                    }
                }
                else {
                    // add new pair borrower
                    $new_borrowers = $this->borrower_resource->create();
                    $new_response_time = $response_time + $this->borrower_resource->getResponseTime();
                    if (is_array($new_borrowers)) {
                        for ($i=0; $i < count($new_borrowers); $i++) {
                            // append new PersonIDs to the current set of $borrowers 
                            $borrowers[$index + $i] = (object) ['PersonID' => $new_borrowers[$i]];
                        }
                        $update = $this->borrower_resource->setResourceId($borrowers[$index]->PersonID)
                            ->update($borrower_data);
                        if ($update->Successful) {
                            $this->csv_service->logImport([
                                'row_num' => $this->row_num,
                                'col_name' => 'Borrower',
                                'col_headers' => $orig_headers,
                                'status' => self::IMPORT_STATUS_SUCCESS,
                                'description' => 'Borrower Info Success',
                                'processing_time' => $new_response_time + $update->Stats->MethodTime,
                            ]);
                        }
                        else {
                            $this->csv_service->logImport([
                                'row_num' => $this->row_num,
                                'col_name' => 'Borrower',
                                'col_headers' => $orig_headers,
                                'status' => self::IMPORT_STATUS_FAILED,
                                'description' => 'Borrower #'.($index + 1) .
                                ' - ['. $update->ErrorCode .']'.$update->ErrorMessage,
                                'processing_time' => $new_response_time + $update->Stats->MethodTime,
                            ]);
                        }
                    }
                    else {
                        $this->csv_service->logImport([
                            'row_num' => $this->row_num,
                            'col_name' => 'Borrower',
                            'col_headers' => $orig_headers,
                            'status' => self::IMPORT_STATUS_NOTICE,
                            'description' => 'Unable to update data for Borrower #'. ($index + 1),
                            'processing_time' => $new_response_time,
                        ]);
                    }
                }
            }
        }
    }

    /**
     * Process customer data
     *
     * @return void
     */
    private function processCustomerData()
    {
        $customer_resource = new \Price\Resources\Customers($this->csv_service->price_config);
        $customers = $customer_resource->get();
        $response_time = $customer_resource->getResponseTime();
        $data = $this->csv_service->getResourceProperty('customer_data');
        if (!$this->checkEmpty($customers)) {
            foreach ($data as $index => $customer_data) {
                $orig_headers = $this->csv_service->getOriginalHeaders(
                    array_keys($customer_data),
                    'Customer',
                    $index
                );
                if (isset($customers[$index])) {
                    $update = $customer_resource->setResourceId($customers[$index]->CustomerID)
                    ->update($customer_data);
                    if ($update->Successful) {
                        $this->csv_service->logImport([
                            'row_num' => $this->row_num,
                            'col_name' => 'Borrower',
                            'col_headers' => $orig_headers,
                            'status' => self::IMPORT_STATUS_SUCCESS,
                            'description' => 'Borrower Info Success',
                            'processing_time' => $response_time + $update->Stats->MethodTime,
                        ]);
                    }
                    else {
                        $this->csv_service->logImport([
                            'row_num' => $this->row_num,
                            'col_name' => 'Borrower',
                            'col_headers' => $orig_headers,
                            'status' => self::IMPORT_STATUS_FAILED,
                            'description' => 'Borrower #'.($index + 1) .' - ['. $update->ErrorCode .']'. $update->ErrorMessage,
                            'processing_time' => $response_time + $update->Stats->MethodTime,
                        ]);
                    }
                }
                else {
                    $this->csv_service->logImport([
                        'row_num' => $this->row_num,
                        'col_name' => 'Borrower',
                        'col_headers' => $orig_headers,
                        'status' => self::IMPORT_STATUS_NOTICE,
                        'description' => 'Unable to update data for Borrower #'.($index + 1),
                        'processing_time' => $response_time,
                    ]);
                }
            }
        }
    }

    /**
     * Process subject property
     *
     * @return void
     */
    private function processSubjectProperty()
    {
        $subj_property_id = $this->process_string->handle([
            'DataLanguage' => 'ihFind(,Loan,Property_id)'
        ]);
        $subj_property_customer_id = $this->process_string->handle([
            'DataLanguage' => 'IhFind(,Loan,Customer_ID)'
        ]);
        $process_string_time = $this->process_string->getResponseTime();
        if (is_numeric($subj_property_id)) {
            $data = $this->csv_service->getResourceProperty('subject_property_data');
            if (isset($data[0])) {
                $mailing_address_headers = [
                    'Mailing_Address',
                    'Mailing_City',
                    'Mailing_State',
                    'Mailing_Zip',
                ];
                $orig_headers = $this->csv_service->getOriginalHeaders(
                    array_keys($data[0]),
                    'Property'
                );
                // remove mailing address from subject property address headers
                $arr_headers = explode(',', $orig_headers);
                $orig_headers = implode(',', array_diff(
                    $arr_headers,
                    $mailing_address_headers
                ));
                $property_resource = new \Price\Resources\Properties(
                    $this->csv_service->price_config
                );
                $response_time = $process_string_time + $property_resource
                    ->getResponseTime();
                if (isset($data[0]['Appraised_Value'])) {
                    $data[0]['Appraised_Value'] = number_format(
                        $data[0]['Appraised_Value'], 2
                    );
                }
                $data[0]['customer_id'] = $subj_property_customer_id;
                $update = $property_resource->setResourceId($subj_property_id)
                        ->update($data[0]);
                if ($update->Successful) {
                    $this->csv_service->logImport([
                        'row_num' => $this->row_num,
                        'col_name' => 'SubjectProperty',
                        'col_headers' => $orig_headers,
                        'status' => self::IMPORT_STATUS_SUCCESS,
                        'description' => 'Subject Property Data Success',
                        'processing_time' => $response_time + $update->Stats->MethodTime,
                    ]);
                }
                else {
                    $this->csv_service->logImport([
                        'row_num' => $this->row_num,
                        'col_name' => 'SubjectProperty',
                        'col_headers' => $orig_headers,
                        'status' => self::IMPORT_STATUS_FAILED,
                        'description' => 'Subject Property - ['. $update->ErrorCode .']'.
                        $update->ErrorMessage,
                        'processing_time' => $response_time + $update->Stats->MethodTime,
                    ]);
                }
                $this->processMailingAddress($data);
            }
        }
    }
    
    /**
     * Process Mailing Address
     *
     * @param array $data
     * @return void
     */
    private function processMailingAddress(array $data)
    {
        // update mailing address if provided
        if (isset($data[0]['Mailing_Address'])) {
            $property_resource = new \Price\Resources\Properties(
                $this->csv_service->price_config
            );
            $mailing_address_id = $this->process_string->handle([
                'DataLanguage' => 'ihFind(ihInfo(GetBorrowerNumber,1),Customer,Mailing_Address_Id)'
            ]);
            $process_string_time = $this->process_string->getResponseTime();
            if (is_numeric($mailing_address_id)) {
                $update = $property_resource->setResourceId($mailing_address_id)
                    ->update([
                        "customer_id" => $data[0]['customer_id'],
                        "Address" => $data[0]['Mailing_Address'],
                        "City" => isset($data[0]['Mailing_City']) ? $data[0]['Mailing_City'] : '',
                        "State" => isset($data[0]['Mailing_State']) ? $data[0]['Mailing_State'] : '',
                        "Zip" => isset($data[0]['Mailing_Zip']) ? $data[0]['Mailing_Zip'] : '',
                    ]);
                if ($update->Successful) {
                    $this->csv_service->logImport([
                        'row_num' => $this->row_num,
                        'col_name' => 'MailingAddress',
                        'col_headers' => 'Mailing_Address,Mailing_City,Mailing_State,Mailing_Zip',
                        'status' => self::IMPORT_STATUS_SUCCESS,
                        'description' => 'Mailing Address Data Success',
                        'processing_time' => $process_string_time + $update->Stats->MethodTime,
                    ]);
                }
                else {
                    $this->csv_service->logImport([
                        'row_num' => $this->row_num,
                        'col_name' => 'MailingAddress',
                        'col_headers' => 'Mailing_Address,Mailing_City,Mailing_State,Mailing_Zip',
                        'status' => self::IMPORT_STATUS_FAILED,
                        'description' => 'Mailing Address - ['. $update->ErrorCode .']'.
                        $update->ErrorMessage,
                        'processing_time' => $process_string_time + $update->Stats->MethodTime,
                    ]);
                }
            }
        }
    }

    /**
     * Process Loan Data
     *
     * @return void
     */
    private function processLoanData()
    {
        $data = $this->csv_service->getResourceProperty('loan_data');
        if (isset($data[0])) {
            $orig_headers = $this->csv_service->getOriginalHeaders(
                array_keys($data[0]),
                'Loan'
            );

            // set related field if filled
            if (isset($data[0]['MortgageInsuranceZeroDueAtClosing'])) {
                $data[0]['MortgageInsuranceRefund'] = 
                    $data[0]['MortgageInsuranceZeroDueAtClosing'] == 'Yes' ? 
                        'Y' : 'N';
            }

            $update = $this->loan_resource->update($data[0]);
            if (!isset($update->Successful)) {
                throw new \Exception("Unable to update loan data");
            }

            if ($update->Successful) {
                $this->csv_service->logImport([
                    'row_num' => $this->row_num,
                    'col_name' => 'LoanData',
                    'col_headers' => $orig_headers,
                    'status' => self::IMPORT_STATUS_SUCCESS,
                    'description' => 'Loan Data Success',
                    'processing_time' => $update->Stats->MethodTime,
                ]);
            }
            else {
                $this->csv_service->logImport([
                    'row_num' => $this->row_num,
                    'col_name' => 'LoanData',
                    'col_headers' => $orig_headers,
                    'status' => self::IMPORT_STATUS_FAILED,
                    'description' => 'Loan Data - ['. $update->ErrorCode .']'. $update->ErrorMessage,
                    'processing_time' => $update->Stats->MethodTime,
                ]);
            }
        }
    }

    /**
     * Process Loan2 Data
     *
     * @return void
     */
    private function processLoan2Data()
    {
        $data = $this->csv_service->getResourceProperty('adjustment_data');
        if (isset($data[0])) {
            $orig_headers = $this->csv_service->getOriginalHeaders(
                array_keys($data[0]),
                'Loan2'
            );
            $price_adjusments = [];
            if (isset($data[0]['wildcards']) && count($data[0]['wildcards'])) {
                $price_adjusments['LockConfirmData'] = [];
                foreach ($data[0]['wildcards'] as $index => $adjustment) {
                    $ctr = $index + 1;
                    $price_adjusments['LockConfirmData'] = array_merge(
                        $price_adjusments['LockConfirmData'],
                        [
                            [
                                "SectionData" => 'CommitPriceAdjustments '.$ctr,
                                "SectionField" => 'Name',
                                "SectionValue" => isset(
                                    $adjustment['Price_Adjustment_Description']
                                ) ? $adjustment['Price_Adjustment_Description'] : "",
                            ],
                            [
                                "SectionData" => 'CommitPriceAdjustments '.$ctr,
                                "SectionField" => 'Amount',
                                "SectionValue" => isset(
                                    $adjustment['Price_Adjustment_Value']
                                ) ? $adjustment['Price_Adjustment_Value'] : "0",
                            ],
                            [
                                "SectionData" => 'CommitPriceAdjustments '.$ctr,
                                "SectionField" => 'Enabled',
                                "SectionValue" => isset(
                                    $adjustment['Price_Adjustment']
                                ) ? $adjustment['Price_Adjustment'] : "0",
                            ],
                        ]
                    );
                }
            }
            unset($data[0]['wildcards']);
            $data[0] = array_merge($data[0], $price_adjusments);
            // due to stripping of prefix, other field name that includes the
            // same as the prefix, we need rename those keys to its correct key
            if (isset($data[0]['Base_Price'])) {
                $data[0]['Commit_Base_Price'] = $data[0]['Base_Price'];
                unset($data[0]['Base_Price']);
            }

            $adjustment_resource = new \Price\Resources\Adjustment(
                $this->csv_service->price_config
            );
            $update = $adjustment_resource->update($data[0]);
            if ($update->Successful) {
                $this->csv_service->logImport([
                    'row_num' => $this->row_num,
                    'col_name' => 'Loan2Data',
                    'col_headers' => $orig_headers,
                    'status' => self::IMPORT_STATUS_SUCCESS,
                    'description' => 'Loan2 Data Success',
                    'processing_time' => $update->Stats->MethodTime,
                ]);
            }
            else {
                $this->csv_service->logImport([
                    'row_num' => $this->row_num,
                    'col_name' => 'Loan2Data',
                    'col_headers' => $orig_headers,
                    'status' => self::IMPORT_STATUS_FAILED,
                    'description' => 'Loan2 Data - ['. $update->ErrorCode .']'. $update->ErrorMessage,
                    'processing_time' => $update->Stats->MethodTime,
                ]);
            }
        }
    }

    /**
     * Process Loan Servicing Data
     *
     * @return void
     */
    private function processLoanServicingData()
    {
        $data = $this->csv_service->getResourceProperty('loan_servicing_data');
        if (isset($data[0])) {
            $orig_headers = $this->csv_service->getOriginalHeaders(
                array_keys($data[0]),
                'LoanServicing'
            );
            $loan_servicing = new \Price\Resources\LoanServicing(
                $this->csv_service->price_config
            );
            $update = $loan_servicing->update($data[0]);
            if ($update->Successful) {
                $this->csv_service->logImport([
                    'row_num' => $this->row_num,
                    'col_name' => 'LoanServicingData',
                    'col_headers' => $orig_headers,
                    'status' => self::IMPORT_STATUS_SUCCESS,
                    'description' => 'Loan Servicing Data Success',
                    'processing_time' => $update->Stats->MethodTime,
                ]);
            }
            else {
                $this->csv_service->logImport([
                    'row_num' => $this->row_num,
                    'col_name' => 'LoanServicingData',
                    'col_headers' => $orig_headers,
                    'status' => self::IMPORT_STATUS_FAILED,
                    'description' => 'Loan Servicing Data - ['. $update->ErrorCode .']'.
                    $update->ErrorMessage,
                    'processing_time' => $update->Stats->MethodTime,
                ]);
            }
        }
    }

    /**
     * Process Loan HMDA Data
     *
     * @return void
     */
    private function processLoanHmdaData()
    {
        $data = $this->csv_service->getResourceProperty('loan_hmda_data');
        if (isset($data[0])) {
            $orig_headers = $this->csv_service->getOriginalHeaders(
                array_keys($data[0]),
                'LoanHMDA'
            );
            $loan_hmda = new \Price\Resources\LoanHmda($this->csv_service->price_config);
            $update = $loan_hmda->update($data[0]);
            if ($update->Successful) {
                $this->csv_service->logImport([
                    'row_num' => $this->row_num,
                    'col_name' => 'LoanHMDAData',
                    'col_headers' => $orig_headers,
                    'status' => self::IMPORT_STATUS_SUCCESS,
                    'description' => 'Loan HMDA Data Success',
                    'processing_time' => $update->Stats->MethodTime,
                ]);
            }
            else {
                $this->csv_service->logImport([
                    'row_num' => $this->row_num,
                    'col_name' => 'LoanHMDAData',
                    'col_headers' => $orig_headers,
                    'status' => self::IMPORT_STATUS_FAILED,
                    'description' => 'Loan HMDA Data - ['. $update->ErrorCode .']'. $update->ErrorMessage,
                    'processing_time' => $update->Stats->MethodTime,
                ]);
            }
        }
    }

    /**
     * Process Loan Correspondent Data
     *
     * @return void
     */
    private function processLoanCorrespondentData()
    {
        $data = $this->csv_service->getResourceProperty('loan_correspondent_data');
        if (isset($data[0])) {
            $orig_headers = $this->csv_service->getOriginalHeaders(
                array_keys($data[0]),
                'LoanCorrespondent'
            );
            $loan_correspondent = new \Price\Resources\LoanCorrespondent(
                $this->csv_service->price_config
            );
            $update = $loan_correspondent->update($data[0]);
            if ($update->Successful) {
                $this->csv_service->logImport([
                    'row_num' => $this->row_num,
                    'col_name' => 'LoanCorrespondentData',
                    'col_headers' => $orig_headers,
                    'status' => self::IMPORT_STATUS_SUCCESS,
                    'description' => 'Loan correspondent Data Success',
                    'processing_time' => $update->Stats->MethodTime,
                ]);
            }
            else {
                $this->csv_service->logImport([
                    'row_num' => $this->row_num,
                    'col_name' => 'LoanCorrespondentData',
                    'col_headers' => $orig_headers,
                    'status' => self::IMPORT_STATUS_FAILED,
                    'description' => 'Loan correspondent Data - ['. $update->ErrorCode .']'.
                    $update->ErrorMessage,
                    'processing_time' => $update->Stats->MethodTime,
                ]);
            }
        }
    }

    /**
     * Process VirtualData
     *
     * @return void
     */
    private function processVirtualData()
    {
        $data = $this->csv_service->getResourceProperty('virtual_data');
        if (count($data)) {
            $orig_headers = $this->csv_service->getOriginalHeaders(
                array_keys($data[0]),
                'VirtualData'
            );
            $virtual_data_resource = new \Price\Resources\VirtualData(
                $this->csv_service->price_config
            );
            $update = $virtual_data_resource->update($data[0]);
            if ($update->Successful) {
                $this->csv_service->logImport([
                    'row_num' => $this->row_num,
                    'col_name' => 'VirtualData',
                    'col_headers' => $orig_headers,
                    'status' => self::IMPORT_STATUS_SUCCESS,
                    'description' => 'Virtual Data Success',
                    'processing_time' => $update->Stats->MethodTime,
                ]);
            }
            else {
                $this->csv_service->logImport([
                    'row_num' => $this->row_num,
                    'col_name' => 'VirtualData',
                    'col_headers' => $orig_headers,
                    'status' => self::IMPORT_STATUS_FAILED,
                    'description' => 'Virtual Data - ['.$update->ErrorCode.']'. $update->ErrorMessage,
                    'processing_time' => $update->Stats->MethodTime,
                ]);
            }
        }
    }

    /**
     * Process ExtraData
     *
     * @return void
     */
    private function processExtraData()
    {
        $data = $this->csv_service->getResourceProperty('extra_data');
        if (count($data)) {
            $extra_data_resource = new \Price\Resources\ExtraData(
                $this->csv_service->price_config
            );
            foreach ($data[0] as $key => $value) {
                // Remove space and special characters
                $extra_data_name = preg_replace(
                    '/[^A-Za-z0-9\-]/', '', $key
                );
                $update = $extra_data_resource->update([
                    'ExtraDataName' => $extra_data_name, 
                    'ExtraDataValue' => $value,
                    'RowNumberID' => '',
                ]);

                if ($update->Successful) {
                    $this->csv_service->logImport([
                        'row_num' => $this->row_num,
                        'col_name' => 'ExtraData',
                        'col_headers' => 'ExtraData_'.$key,
                        'status' => self::IMPORT_STATUS_SUCCESS,
                        'description' => 'Extra Data Success',
                        'processing_time' => $update->Stats->MethodTime,
                    ]);
                }
                else {
                    $this->csv_service->logImport([
                        'row_num' => $this->row_num,
                        'col_name' => 'ExtraData',
                        'col_headers' => 'ExtraData_'.$key,
                        'status' => self::IMPORT_STATUS_FAILED,
                        'description' => 'Extra Data - ['. $update->ErrorCode .']'. $update->ErrorMessage,
                        'processing_time' => $update->Stats->MethodTime,
                    ]);
                }
            }
        }
    }

    /**
     * Process Date Data
     *
     * @return void
     */
    private function processDateData()
    {
        $data = $this->csv_service->getResourceProperty('date_data');
        if (count($data)) {
            $add_or_update_date = new \Price\Misc\AddOrUpdateDate(
                $this->csv_service->price_config
            );
            foreach ($data[0] as $key => $value) {
                $date_name = str_replace('_', ' ', $key);
                $update = $add_or_update_date->handle([
                    'DateName' => $date_name,
                    'DateValue' => $value,
                ]);

                if ($update->Successful) {
                    $this->csv_service->logImport([
                        'row_num' => $this->row_num,
                        'col_name' => 'DateData',
                        'col_headers' => 'Date_Data_'.$key,
                        'status' => self::IMPORT_STATUS_SUCCESS,
                        'description' => 'Date Data Success',
                        'processing_time' => $update->Stats->MethodTime,
                    ]);
                }
                else {
                    $this->csv_service->logImport([
                        'row_num' => $this->row_num,
                        'col_name' => 'DateData',
                        'col_headers' => $date_name,
                        'status' => self::IMPORT_STATUS_FAILED,
                        'description' => 'Date Data - ['. $update->ErrorCode .']'. $update->ErrorMessage,
                        'processing_time' => $update->Stats->MethodTime,
                    ]);
                }
            }
        }
    }

    /**
     * Process Fee Data
     *
     * @return void
     */
    private function processFeeData()
    {
        try {
            $data = $this->csv_service->getResourceProperty('fee_data');
            if (count($data)) {
                $fees = new \Price\Resources\Fees(
                    $this->csv_service->price_config
                );
                $loan_fees = new \Price\Resources\LoanFees(
                    $this->csv_service->price_config
                );
                $available_fees = collect($fees->get());
                $fee_proc_time = $fees->getResponseTime();
                if (!$available_fees->count()) {
                    \Log::error('[DOT][Fees] Unable to get fees.');
                    $this->csv_service->logImport([
                        'row_num' => $this->row_num,
                        'col_name' => 'FeeData',
                        'col_headers' => 'ALL',
                        'status' => self::IMPORT_STATUS_FAILED,
                        'description' => 'Unable to get fees',
                        'processing_time' => $fee_proc_time,
                    ]);
                }
                else {
                    $fee_data = [];
                    // collect all fee field
                    foreach ($data[0] as $key => $value) {
                        $col_header = explode('_', $key);
                        list(, $fee_id) = $col_header;
                        // store original headers for logs purposes
                        $fee_data[$fee_id]['headers'][] = $key;
                        // exploded header that > 3 contains most of PRICE fields
                        if (count($col_header) > 3) {
                            // Parse the PRICE field
                            $field = str_replace(' ', '_', ucwords(end($col_header)));
                            $fee_data[$fee_id][$field] = $value;
                        }
                        else {
                            // fee amount
                            $fee_data[$fee_id]['Total'] = $value;
                        }
                    }
    
                    // excute fee datas to PRICE
                    $existing_loan_fees = $loan_fees->get();
                    $process_time = $fee_proc_time +  $loan_fees->getResponseTime();
                    foreach ($fee_data as $key => $attributes) {
                        $headers = implode(',', $attributes['headers']);
                        // check if fee exists
                        $fee = $available_fees->where('FeeID', $key)->first();
                        if (!is_null($fee)) {
                            // check if Fee already exists in the loan
                            $current_loan_fee = $existing_loan_fees->where(
                                'Fee_ID', $fee->FeeID
                            )->first();
                            if (!is_null($current_loan_fee)) {
                                // Loan Fee exists
                                $loan_fee_id = $current_loan_fee['Loan_Fee_ID'];
                            }
                            else {
                                // Loan Fee not exists
                                $loan_fee_id = $loan_fees->setQueryParams([
                                    'FeeID' => $fee->FeeID,
                                ])->create();
                            }
                            $update = $loan_fees->setResourceId($loan_fee_id)
                                ->update($attributes);
                            if ($update->Successful) {
                                $this->csv_service->logImport([
                                    'row_num' => $this->row_num,
                                    'col_name' => 'FeeData',
                                    'col_headers' => $headers,
                                    'status' => self::IMPORT_STATUS_SUCCESS,
                                    'description' => 'Fee data success',
                                    'processing_time' => $update->Stats->MethodTime,
                                ]);
                            }
                            else {
                                \Log::error('[DOT][Fees] Unable to add fee.');
                                $this->csv_service->logImport([
                                    'row_num' => $this->row_num,
                                    'col_name' => 'FeeData',
                                    'col_headers' => $headers,
                                    'status' => self::IMPORT_STATUS_FAILED,
                                    'description' => 'Unable to add fee.',
                                    'processing_time' => $update->Stats->MethodTime,
                                ]);
                            }
                        }
                        else {
                            $this->csv_service->logImport([
                                'row_num' => $this->row_num,
                                'col_name' => 'FeeData',
                                'col_headers' => $headers,
                                'status' => self::IMPORT_STATUS_FAILED,
                                'description' => 'Unable to find fee',
                                'processing_time' => $process_time,
                            ]);
                        }
                    }
                }
            }
        } catch (\Exception $e) {
            \Log::info("FEE ". $e->getMessage());
        }
    }

    private function processCompanyData()
    {
        $data = $this->csv_service->getResourceProperty('company_data');
        if (count($data)) {
            $orig_headers = $this->csv_service->getOriginalHeaders(
                array_keys($data[0]),
                'Company'
            );
            $company_id = $this->process_string->handle([
                'DataLanguage' => 'ihFind(,Loan,ihFind(,Loan,Broker_Company_ID))'
            ]);
            $process_string_time = $this->process_string->getResponseTime();
            if (is_numeric($company_id)) {
                $company_resource = new \Price\Resources\Companies(
                    $this->csv_service->price_config
                );
                $update = $company_resource->setResourceId($company_id)
                        ->update($data[0]);
                if ($update->Successful) {
                    $this->csv_service->logImport([
                        'row_num' => $this->row_num,
                        'col_name' => 'CompanyData',
                        'col_headers' => $orig_headers,
                        'status' => self::IMPORT_STATUS_SUCCESS,
                        'description' => 'Company Data Success',
                        'processing_time' => $process_string_time + $update->Stats->MethodTime,
                    ]);
                }
                else {
                    $this->csv_service->logImport([
                        'row_num' => $this->row_num,
                        'col_name' => 'CompanyData',
                        'col_headers' => $orig_headers,
                        'status' => self::IMPORT_STATUS_FAILED,
                        'description' => 'Company Data - ['. $update->ErrorCode .']'.
                        $update->ErrorMessage,
                        'processing_time' => $process_string_time + $update->Stats->MethodTime,
                    ]);
                }
            }
        }
    }

    private function processLoanLicense()
    {
        $data = $this->csv_service->getResourceProperty('loan_license_data');
        if (count($data)) {
            $orig_headers = $this->csv_service->getOriginalHeaders(
                array_keys($data[0]),
                'LoanLicense'
            );
            $loan_license = new \Price\Resources\LoanLicense(
                $this->csv_service->price_config
            );
            $attributes = [];
            $license_prefixes = [
                'Loan_Lender_' => $loan_license::DATA_FROM_LENDER,
                'LoanOfficer' => $loan_license::DATA_FROM_LOAN_OFFICER,
                'Broker' => $loan_license::DATA_FROM_BROKER_COMPANY,
                'BrokerPerson' => $loan_license::DATA_FROM_BROKER_PERSON,
            ];
            foreach ($data[0] as $key => $value) {
                // get the prefix
                $prefix = substr($key, 0, strpos($key, 'License'));
                // check if prefix is valid
                if (isset($license_prefixes[$prefix])) {
                    $lists = isset($attributes[$prefix]) ? 
                        $attributes[$prefix] : 
                        ['DataFrom' => $license_prefixes[$prefix]];
                    // replace the prefix to empty string to set the right field
                    // and remove _ to match PRICE field name
                    $attributes[$prefix] = array_merge($lists, [
                        str_replace('_', '', str_replace($prefix, '', $key)) => $value
                    ]);
                }
            }
            // Execute the update for Loan License data
            foreach ($attributes as $prefix => $data) {
                $update = $loan_license->update($data);
                if ($update->Successful) {
                    $this->csv_service->logImport([
                        'row_num' => $this->row_num,
                        'col_name' => 'LoanLicenseData',
                        'col_headers' => $orig_headers,
                        'status' => self::IMPORT_STATUS_SUCCESS,
                        'description' => 'Loan License '. $prefix .' Success',
                        'processing_time' => $update->Stats->MethodTime,
                    ]);
                }
                else {
                    $this->csv_service->logImport([
                        'row_num' => $this->row_num,
                        'col_name' => 'LoanLicenseData',
                        'col_headers' => $orig_headers,
                        'status' => self::IMPORT_STATUS_FAILED,
                        'description' => 'Loan License '. $prefix .' - ['. $update->ErrorCode .']'.
                        $update->ErrorMessage,
                        'processing_time' => $update->Stats->MethodTime,
                    ]);
                }
            }
        }
    }

    private function processInvestorFeatureCode()
    {
        try {
            $data = $this->csv_service->getResourceProperty('investor_code_data');
            if (count($data)) {
                $orig_headers = $this->csv_service->getOriginalHeaders(
                    array_keys($data[0]),
                    'InvestorFeatureCode'
                );
                $investor_code = new \Price\Resources\InvestorFeatureCode(
                    $this->csv_service->price_config
                );
                $investors = ['FNMA' => '0', 'FHLMC' => '1'];
                foreach ($data[0] as $key => $value) {
                    list($investor) = explode('_', $key);
                    if (isset($investors[$investor])) {
                        $update = $investor_code->update([
                            'FEATURECODETYPE' => $investors[$investor],
                            'FEATURECODES' => $value,
                        ]);
                        if (isset($update->Successful) && $update->Successful) {
                            $this->csv_service->logImport([
                                'row_num' => $this->row_num,
                                'col_name' => 'InvestorFeatureCode',
                                'col_headers' => $orig_headers,
                                'status' => self::IMPORT_STATUS_SUCCESS,
                                'description' => 'Investor Feature Code Success',
                                'processing_time' => $update->Stats->MethodTime,
                            ]);
                        }
                        else {
                            if (is_object($update)) {
                                $error_desc = 'Investor Feature Code - ['. $update->ErrorCode .']'.
                                $update->ErrorMessage;
                                $proc_time = $update->Stats->MethodTime;
                            }
                            else {
                                $uuid = substr(Uuid::generate()->string, 0, 8);
                                $error_desc = 'Server error. Reference ID:'.$uuid;
                                \Log::error("[DOT][ERROR][".$uuid."] " .$update);
                                $proc_time = 0;
                            }
                            $this->csv_service->logImport([
                                'row_num' => $this->row_num,
                                'col_name' => 'InvestorFeatureCode',
                                'col_headers' => $orig_headers,
                                'status' => self::IMPORT_STATUS_FAILED,
                                'description' => $error_desc,
                                'processing_time' => $proc_time,
                            ]);
                        }
                    }
                    else {
                        $this->csv_service->logImport([
                            'row_num' => $this->row_num,
                            'col_name' => 'InvestorFeatureCode',
                            'col_headers' => $orig_headers,
                            'status' => self::IMPORT_STATUS_FAILED,
                            'description' => 'Investor '.$investor.' is not supported.',
                        ]);
                    }
                }
            }
        } catch (\Exception $e) {
            $uuid = substr(Uuid::generate()->string, 0, 8);
            \Log::error("[DOT][ERROR][".$uuid."] ". $e->getMessage());
            $this->csv_service->logImport([
                'row_num' => $this->row_num,
                'col_name' => 'InvestorFeatureCode',
                'col_headers' => $orig_headers,
                'status' => self::IMPORT_STATUS_FAILED,
                'description' => 'Server error. Reference ID: '. $uuid,
            ]);
        }
    }

    /**
     * Loan Correspondent Fee
     *
     * @return void
     */
    private function processLoanCorrespondentFee()
    {
        try {
            $data = $this->csv_service->getResourceProperty(
                'loan_correspondent_fee_data'
            );
            if (isset($data[0]['wildcards']) && count($data[0]['wildcards'])) {
                foreach ($data[0]['wildcards'] as $index => $fee) {
                    $attributes = [];
                    $orig_headers = $this->csv_service->getOriginalHeaders(
                        array_keys($data[0]['wildcards'][$index]),
                        'LoanCorrespondentFee',
                        $index
                    );
                    $attributes = [
                        'LoanFeeID' => isset($fee['LoanFeeID']) ? $fee['LoanFeeID'] : 0
                    ];
                    if (isset($fee['FeeID']))
                        $attributes['FeeID'] = (float) $fee['FeeID'];
                    if (isset($fee['FeeDescription']))
                        $attributes['Name'] = $fee['FeeDescription'];
                    if (isset($fee['FeeAmount']))
                        $attributes['Value'] = (float) $fee['FeeAmount'];

                    $correspondent_fees = new \Price\Resources\LoanCorrespondentFee(
                        $this->csv_service->price_config
                    );
                    $update = $correspondent_fees->update($attributes);
                    if ($update->Successful) {
                        $this->csv_service->logImport([
                            'row_num' => $this->row_num,
                            'col_name' => 'LoanCorrespondentFeeData',
                            'col_headers' => $orig_headers,
                            'status' => self::IMPORT_STATUS_SUCCESS,
                            'description' => 'Correspondent Fee Data Success',
                            'processing_time' => $update->Stats->MethodTime,
                        ]);
                    }
                    else {
                        $this->csv_service->logImport([
                            'row_num' => $this->row_num,
                            'col_name' => 'LoanCorrespondentFeeData',
                            'col_headers' => $orig_headers,
                            'status' => self::IMPORT_STATUS_FAILED,
                            'description' => 'Correspondent Fee Data - ['. $update->ErrorCode .']'. $update->ErrorMessage,
                            'processing_time' => $update->Stats->MethodTime,
                        ]);
                    }
                }
            }
        } catch (\Exception $e) {
            $ref = time();
            $this->csv_service->logImport([
                'row_num' => $this->row_num,
                'col_name' => 'LoanCorrespondentFeeData',
                'col_headers' => $orig_headers,
                'status' => self::IMPORT_STATUS_FAILED,
                'description' => '[Ref #'.$ref.'] System error. Please contact support.',
                'processing_time' => 0,
            ]);
            \Log::error("[DOT][CORRESPONDENT FEE] Ref #$ref Message: ". $e->getMessage());
        }
    }

    /**
     * Loan Correspondent Adjustment
     *
     * @return void
     */
    private function processLoanCorrespondentAdjustment()
    {
        try {
            $data = $this->csv_service->getResourceProperty(
                'loan_correspondent_adjustment_data'
            );
            if (isset($data[0]['wildcards']) && count($data[0]['wildcards'])) {
                foreach ($data[0]['wildcards'] as $index => $adjustment) {
                    $attributes = [];
                    $orig_headers = $this->csv_service->getOriginalHeaders(
                        array_keys($data[0]['wildcards'][$index]),
                        'LoanCorrespondentAdjustment',
                        $index
                    );
                    $attributes = [
                        'AdjustmentID' => isset($adjustment['AdjustmentID']) ? $adjustment['AdjustmentID'] : ($index+1),
                        'Enabled' => isset($adjustment['PriceAdjustmentEnabled']) ? (bool) $adjustment['PriceAdjustmentEnabled'] : true,
                    ];
                    if (isset($adjustment['PriceAdjustmentDescription']))
                        $attributes['AdjustmentName'] = $adjustment['PriceAdjustmentDescription'];
                    if (isset($adjustment['PriceAdjustmentAmount']))
                        $attributes['Amount'] = (float) $adjustment['PriceAdjustmentAmount'];

                    $correspondent_adj = new \Price\Resources\LoanCorrespondentAdjustment(
                        $this->csv_service->price_config
                    );
                    $update = $correspondent_adj->update($attributes);
                    if ($update->Successful) {
                        $this->csv_service->logImport([
                            'row_num' => $this->row_num,
                            'col_name' => 'LoanCorrespondentAdjustmentData',
                            'col_headers' => $orig_headers,
                            'status' => self::IMPORT_STATUS_SUCCESS,
                            'description' => 'Correspondent Adjustment Data Success',
                            'processing_time' => $update->Stats->MethodTime,
                        ]);
                    }
                    else {
                        $this->csv_service->logImport([
                            'row_num' => $this->row_num,
                            'col_name' => 'LoanCorrespondentAdjustmentData',
                            'col_headers' => $orig_headers,
                            'status' => self::IMPORT_STATUS_FAILED,
                            'description' => 'Correspondent Adjustment Data - ['. $update->ErrorCode .']'. $update->ErrorMessage,
                            'processing_time' => $update->Stats->MethodTime,
                        ]);
                    }
                }
            }
        } catch (\Exception $e) {
            $ref = time();
            $this->csv_service->logImport([
                'row_num' => $this->row_num,
                'col_name' => 'LoanCorrespondentAdjustmentData',
                'col_headers' => $orig_headers,
                'status' => self::IMPORT_STATUS_FAILED,
                'description' => '[Ref #'.$ref.'] System error. Please contact support.',
                'processing_time' => 0,
            ]);
            \Log::error("[DOT][CORRESPONDENT ADJUSTMENT] Ref #$ref Message: ". $e->getMessage());
        }
    }
}
