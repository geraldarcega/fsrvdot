<?php

namespace App\Dot;

use App\Customer;

class ImporterConfig
{
    protected $price_db;
    protected $customer;
    protected $price_user;
    protected $current_row;

    public function __construct(
        $price_db,
        Customer $customer,
        array $price_user
    )
    {
        $this->price_db = $price_db;
        $this->customer = $customer;
        $this->price_user = $price_user;
    }

    public function priceDb()
    {
        return $this->price_db;
    }

    public function customer()
    {
        return $this->customer;
    }

    public function priceUser()
    {
        return $this->price_user;
    }

    public function setCurrentRow(int $row)
    {
        $this->current_row = $row;
        return $this;
    }

    public function currentRow()
    {
        return $this->current_row;
    }
}
