<?php
namespace PHPFacile\DataPackage\TableSchema\Db\Service;

class DataPackageService
{

    /**
     * Returns a list of joins to be performed so as to link 2 tables according
     * to a data package description
     *
     * @param array        $dataPackage                     Data package description
     * @param string       $fromResourceName                Name of the 1st table
     * @param string       $toResourceName                  Name of the 2nd table
     * @param array|string $requiredAdditionalResourceNames Array of additionnal required tables
     *
     * @return array Array of array with keys
     *               'resource' => name of the table involved in join
     *               'on' => array of join clauses as string like "table1.field1 = table2.field2"
     * @throws \Exception
     */
    protected function getJoinsFromToWithRequiredTables($dataPackage, $fromResourceName, $toResourceName, $requiredAdditionalResourceNames)
    {
        if (true === is_string($requiredAdditionalResourceNames)) {
            $requiredAdditionalResourceNames = [$requiredAdditionalResourceNames];
        }

        $joins = $this->getJoinsFromTo($dataPackage, $fromResourceName, $toResourceName, null);

        if (true === is_array($dataPackage)) {
            $dataPackage = json_decode(json_encode($dataPackage));
        }

        foreach ($requiredAdditionalResourceNames as $requiredAdditionalResourceName) {
            if (false === array_key_exists($requiredAdditionalResourceName, $joins)) {
                $linked = false;
                foreach ($joins as $tableName => $join) {
                    // Is there a link between $tableName and $requiredAdditionalResourceName ??
                    // --
                    // Check foreignKeys from $tableName to $requiredAdditionalResourceName.
                    $tableNameIdx = null;
                    foreach ($dataPackage->resources as $idx => $resource) {
                        if ($tableName === $resource->name) {
                            $tableNameIdx = $idx;
                            break;
                        }
                    }

                    $resource = $dataPackage->resources[$tableNameIdx];
                    if (true === property_exists($resource, 'foreignKeys')) {
                        foreach ($resource->foreignKeys as $fkCfg) {
                            if ($requiredAdditionalResourceName === $fkCfg->reference->resource) {
                                $linked = true;
                                 // FIXME here we assume $fields are single fields!
                                $on = $fkCfg->reference->resource.'.'.$fkCfg->reference->fields.'='.$resource->name.'.'.$fkCfg->fields;
                                if (false === array_key_exists('on', $joins[$requiredAdditionalResourceName])) {
                                    $joins[$requiredAdditionalResourceName]['on'] = [];
                                }

                                $joins[$requiredAdditionalResourceName]['on'][] = $on;
                            }

                            if (true === $linked) {
                                break;
                            }
                        }
                    }

                    if (false === $linked) {
                        // Check foreignKeys from $requiredAdditionalResourceName to $tableName.
                        $requiredAdditionalResourceNameIdx = null;
                        foreach ($dataPackage->resources as $idx => $resource) {
                            if ($requiredAdditionalResourceName === $resource->name) {
                                $requiredAdditionalResourceNameIdx = $idx;
                                break;
                            }
                        }

                        $resource = $dataPackage->resources[$requiredAdditionalResourceNameIdx];
                        if (true === property_exists($resource, 'foreignKeys')) {
                            foreach ($resource->foreignKeys as $fkCfg) {
                                if ($tableName === $fkCfg->reference->resource) {
                                    $linked = true;
                                    // FIXME here we assume $fields are single fields!
                                    $on = $fkCfg->reference->resource.'.'.$fkCfg->reference->fields.'='.$resource->name.'.'.$fkCfg->fields;
                                    if (false === array_key_exists($requiredAdditionalResourceName, $joins)) {
                                        $joins[$requiredAdditionalResourceName] = [];
                                    }

                                    if (false === array_key_exists('on', $joins[$requiredAdditionalResourceName])) {
                                        $joins[$requiredAdditionalResourceName]['on'] = [];
                                    }

                                    $joins[$requiredAdditionalResourceName]['on'][] = $on;
                                }

                                if (true === $linked) {
                                    break;
                                }
                            }
                        }
                    }

                    if (true === $linked) {
                        break;
                    }
                }

                if (false === $linked) {
                    throw new \Exception('Oups');
                }
            }
        }

        return $joins;
    }

    /**
     * Returns a list of joins to be performed so as to link 2 tables according
     * to a data package description
     *
     * @param array        $dataPackage                     Data package description
     * @param string       $fromResourceName                Name of the 1st table
     * @param string       $toResourceName                  Name of the 2nd table
     * @param array|string $requiredAdditionalResourceNames Array of additionnal required tables
     * @param array        $joins                           List of joins to merge with
     *
     * @return array Array of array with keys
     *               'resource' => name of the table involved in join
     *               'on' => array of join clauses as string like "table1.field1 = table2.field2"
     */
    public function getJoinsFromTo($dataPackage, $fromResourceName, $toResourceName, $requiredAdditionalResourceNames = null, $joins = [])
    {
        // FIXME If we could have resourceName as array key it would be more efficient
        if (true === is_array($dataPackage)) {
            $dataPackage = json_decode(json_encode($dataPackage));
        }

        if (null !== $requiredAdditionalResourceNames) {
            return $this->getJoinsFromToWithRequiredTables($dataPackage, $fromResourceName, $toResourceName, $requiredAdditionalResourceNames);
        }

        // TODO Add cache management
        $fromResourceIdx = null;
        foreach ($dataPackage->resources as $idx => $resource) {
            if ($fromResourceName === $resource->name) {
                $fromResourceIdx = $idx;
                break;
            }
        }

        if (null === $fromResourceIdx) {
            // Ignore if the schema is not described in $dataPackage
            // It probably means that it is not useful
            // TODO Check whether we should return null or raise an Exception
            return null;
        }

        $fromResource = $dataPackage->resources[$fromResourceIdx];

        $keptJoins = null;
        // Explore all links "from -> to"
        // but take care we also have to check "to -> from" links (Cf. below)
        if (true === property_exists($resource, 'foreignKeys')) {
            foreach ($resource->foreignKeys as $fkCfg) {
                if ($toResourceName === $fkCfg->reference->resource) {
                    // destination reached
                    // FIXME here we assume $fields are single fields
                    $on = $fkCfg->reference->resource.'.'.$fkCfg->reference->fields.'='.$fromResourceName.'.'.$fkCfg->fields;
                    // Keep in mind that a copy of array is like a clone
                    // modifications made on the copy have no impact
                    // on the original array
                    $keptJoins = $joins;
                    // $keptJoins[$fkCfg->reference->resource]['on'][$on] = $on;
                    $keptJoins[$fkCfg->reference->resource]['on'][] = $on;
                    // That's obviously the shortest
                    return $keptJoins;
                } else {
                    // trying this way... except if the table was already involved in a previous join
                    if (false === array_key_exists($fkCfg->reference->resource, $joins)) {
                        // FIXME here we assume $fields are single fields
                        $on = $fkCfg->reference->resource.'.'.$fkCfg->reference->fields.'='.$fromResourceName.'.'.$fkCfg->fields;
                        // Keep in mind that a copy of array is like a clone
                        // modifications made on the copy have no impact
                        // on the original array
                        $tryedJoins = $joins;
                        // $tryedJoins[$fkCfg->reference->resource]['on'][$on] = $on;
                        $tryedJoins[$fkCfg->reference->resource]['on'][] = $on;
                        $tryedDataPackage = clone $dataPackage;
                        unset($tryedDataPackage->resources[$fromResourceIdx]);
                        // Is it the shortest ??
                        $allJoins = self::getJoinsFromTo($tryedDataPackage, $fkCfg->reference->resource, $toResourceName, null, $tryedJoins);
                        if (null !== $allJoins) {
                            if ((null === $keptJoins)||(count($keptJoins) > count($allJoins))) {
                                $keptJoins = $allJoins;
                            }
                        } // else ignore... wrong way
                    }
                }
            }
        }

        // Explore all links "to -> from"
        foreach ($dataPackage->resources as $idx => $resource) {
            if (($fromResourceIdx !== $idx)&&(true === property_exists($resource, 'foreignKeys'))) {
                foreach ($resource->foreignKeys as $fkCfg) {
                    if ($fromResourceName === $fkCfg->reference->resource) {
                        if ($toResourceName === $resource->name) {
                            // Destination reached !
                            // FIXME here we assume $fields are single fields
                            $on = $fkCfg->reference->resource.'.'.$fkCfg->reference->fields.'='.$resource->name.'.'.$fkCfg->fields;
                            // Keep in mind that a copy of array is like a clone
                            // modifications made on the copy have no impact
                            // on the original array
                            $keptJoins = $joins;
                            // $keptJoins[$resource->name]['on'][$on] = $on;
                            $keptJoins[$resource->name]['on'][] = $on;
                            // That's obviously the shortest
                            return $keptJoins;
                        } else {
                            // Trying this way
                            // FIXME here we assume $fields are single fields
                            $on = $fkCfg->reference->resource.'.'.$fkCfg->reference->fields.'='.$resource->name.'.'.$fkCfg->fields;
                            // Keep in mind that a copy of array is like a clone
                            // modifications made on the copy have no impact
                            // on the original array
                            $tryedJoins = $joins;
                            // $tryedJoins[$resource->name]['on'][$on] = $on;
                            $tryedJoins[$resource->name]['on'][] = $on;
                            $tryedDataPackage = clone $dataPackage;
                            unset($tryedDataPackage->resources[$fromResourceIdx]);
                            // Is it the shortest ??
                            $allJoins = self::getJoinsFromTo($tryedDataPackage, $resource->name, $toResourceName, null, $tryedJoins);
                            if (null !== $allJoins) {
                                if ((null === $keptJoins)||(count($keptJoins) > count($allJoins))) {
                                    $keptJoins = $allJoins;
                                }
                            } // else ignore... wrong way
                        }
                    }
                }
            }
        }

        return $keptJoins;
    }

    /**
     * Return joins required to get values of fields involved in $filter
     * starting from $mainResourceName
     * TODO Manage links between 2 tables involving more than 2 tables
     *
     * @param array  $dataPackage      Data package description
     * @param string $mainResourceName Name of the main table
     * @param array  $filter           Associative array where keys are fully qualified (i.e. including table name) field names
     *                                 and values are ignored but are assumed to contain an expected value (for a query in database)
     *
     * @return array Associative array where keys are name of tables involved
     *               in joins and values are a array with keys
     *               'resource' => name of the table involved in join
     *               'on' => array of join clauses as string like "table1.field1 = table2.field2"
     * @throws Exception If no join serie can be found
     */
    public function getJoins($dataPackage, $mainResourceName, $filter)
    {
        $joins = [];
        foreach ($dataPackage->resources as $resource) {
            $tableName = $resource->name;
            foreach ($resource->schema->fields as $field) {
                $fieldName = $field->name;
                if (true === array_key_exists($tableName.'.'.$fieldName, $filter)) {
                    if ($tableName !== $mainResourceName) {
                        // A join is required.
                        if (false === array_key_exists($tableName, $joins)) {
                            $joins[$tableName] = ['resource' => $tableName];
                        }

                        foreach ($resource->foreignKeys as $fkCfg) {
                            if ($mainResourceName === $fkCfg->reference->resource) {
                                // FIXME here we assume $fields are single fields!
                                $on = $fkCfg->reference->resource.'.'.$fkCfg->reference->fields.'='.$tableName.'.'.$fkCfg->fields;
                                $joins[$tableName]['on'][$on] = $on;
                            }
                        }

                        if (false === array_key_exists('on', $joins[$tableName])) {
                            throw new \Exception('Oups. Unable to build ON clause');
                        }
                    }
                }
            }
        }

        return $joins;
    }

    /**
     * Returns the foreign key field name that can be found in "linked" table
     * that point to a "main" field in a "main" table according to a data package
     * description
     *
     * @param array  $dataPackage           Data package description
     * @param string $mainResourceName      Name of the main table
     * @param string $mainResourceFieldName Name of the field in the main table
     * @param string $linkedResourceName    Name of the table containg a field linked to the main table
     *
     * @return string Name of the field or null if not found
     * @throws Exception In case of "inconsistency" ?
     */
    public function getFKFieldNameInLinkedResource($dataPackage, $mainResourceName, $mainResourceFieldName, $linkedResourceName)
    {
        // Look for $linkedResourceName description.
        $targetResource = null;
        foreach ($dataPackage->resources as $resource) {
            if ($resource->name === $linkedResourceName) {
                $targetResource = $resource;
            }

            if (null !== $targetResource) {
                break;
            }
        }

        if (null === $targetResource) {
             // FIXME raise a NotFoundException?
            throw new \Exception('Not found');
        }

        // Look if in the $targetResource foreign keys we have a link to
        // $mainResourceName . $mainResourceFieldName
        // FIXME Links involving more than 2 tables are not supported.
        foreach ($resource->foreignKeys as $fkCfg) {
            if (($mainResourceName === $fkCfg->reference->resource) && ($mainResourceFieldName === $fkCfg->reference->fields)) {
                // There is a link from $linkedResourceName to $mainResourceName
                // through $fkCfg->fields.
                return $fkCfg->fields;
            }
        }

        // TODO null or NotFoundException ?
        return null;
    }
}
