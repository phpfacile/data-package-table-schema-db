<?php
declare(strict_types=1);

use PHPUnit\Framework\TestCase;

use PHPFacile\DataPackage\TableSchema\Db\Service\DataPackageService;

final class DataPackageServiceTest extends TestCase
{
    protected $dataPackage;
    protected $dataPackageMultiPivot;
    protected $dataPackageService;

    protected function setUp()
    {
        $this->dataPackage = json_decode(json_encode([ // Cf. http://frictionlessdata.io/specs/data-package/
            'resources' => [
                // Cf. https://frictionlessdata.io/specs/table-schema/
                [
                    'name' => 'categories',
                    'schema' => [
                        'fields' => [
                            [
                                'name' => 'id',
                            ],
                            [
                                'name' => 'label',
                            ]
                        ]
                    ]
                ],
                [
                    'name' => 'books',
                    'schema' => [
                        'fields' => [
                            [
                                'name' => 'title',
                            ],
                            [
                                'name' => 'category_id',
                            ],
                            [
                                'name' => 'publication_year',
                            ]
                        ]
                    ],
                    'foreignKeys' => [
                        [
                            'fields' => 'category_id',
                            'reference' =>  [
                                'resource' => 'categories',
                                'fields' => 'id'
                            ]
                        ]
                    ]
                ]
            ]
        ]));


        $this->dataPackageMultiPivot = json_decode(json_encode([ // Cf. http://frictionlessdata.io/specs/data-package/
            'resources' => [
                // Cf. https://frictionlessdata.io/specs/table-schema/
                [
                    'name' => 'authors',
                    'schema' => [
                        'fields' => [
                            [
                                'name' => 'id',
                            ],
                            [
                                'name' => 'name',
                            ]
                        ]
                    ]
                ],
                // For the tests we expect book to be linked by 2 tables
                // (here book_authors and book_set_pivots)
                [
                    'name' => 'books',
                    'schema' => [
                        'fields' => [
                            [
                                'name' => 'id',
                            ],
                            [
                                'name' => 'title',
                            ]
                        ]
                    ]
                ],
                [
                    'name' => 'book_authors',
                    'schema' => [
                        'fields' => [
                            [
                                'name' => 'book_id',
                            ],
                            [
                                'name' => 'author_id',
                            ],
                        ]
                    ],
                    'foreignKeys' => [
                        [
                            'fields' => 'book_id',
                            'reference' => [
                                'resource' => 'books',
                                'fields' => 'id'
                            ]
                        ],
                        [
                            'fields' => 'author_id',
                            'reference' => [
                                'resource' => 'authors',
                                'fields' => 'id'
                            ]
                        ],
                    ]
                ],
                [
                    'name' => 'book_set_pivots',
                    'schema' => [
                        'fields' => [
                            [
                                'name' => 'book_set_id',
                            ],
                            [
                                'name' => 'book_id',
                            ],
                        ]
                    ],
                    'foreignKeys' => [
                        [
                            'fields' => 'book_set_id',
                            'reference' => [
                                'resource' => 'book_sets',
                                'fields' => 'id'
                            ]
                        ],
                        [
                            'fields' => 'book_id',
                            'reference' => [
                                'resource' => 'books',
                                'fields' => 'id'
                            ]
                        ],
                    ]
                ],
                [
                    'name' => 'book_sets',
                    'schema' => [
                        'fields' => [
                            [
                                'name' => 'id'
                            ],
                            [
                                'name' => 'label'
                            ]
                        ]
                    ]
                ]
            ]
        ]));

        $this->dataPackageService = new DataPackageService();
    }

    public function testGetFKFieldNameInLinkedResource()
    {
        $fieldName = $this->dataPackageService->getFKFieldNameInLinkedResource($this->dataPackage, 'categories', 'id', 'books');
        $this->assertEquals('category_id', $fieldName);
    }

    public function testGetJoins()
    {
        $joins = $this->dataPackageService->getJoins($this->dataPackage, 'categories', ['books.publication_year' => 2018]);
        // If we want to be able to get data from categories and perform a filter base on books.publication_year
        // we will have to perform the following joins
        $this->assertEquals(
            [
                'books' => [
                    'resource' => 'books',
                    'on' => [
                        'categories.id=books.category_id' => 'categories.id=books.category_id'
                    ],
                ]
            ],
            $joins);
    }

    public function testGetJoinsFromTo()
    {
        $before = json_encode($this->dataPackageMultiPivot);
        $joins = $this->dataPackageService->getJoinsFromTo($this->dataPackageMultiPivot, 'authors', 'books');
        $after = json_encode($this->dataPackageMultiPivot);
        $this->assertEquals($before, $after, 'Input parameter altered !');

        // Valid but non optimal output
        $this->assertEquals(
            [
                'book_authors' => [
                    'on' => [
                        'authors.id=book_authors.author_id'
                    ],
                ],
                'books' => [
                    'on' => [
                        'books.id=book_authors.book_id'
                    ],
                ],
            ],
            $joins);

        // Optimal output if we consider we only need book_id
        /*
        $this->assertEquals(
            [
                'book_authors' => [
                    'on' => [
                        'authors.id=book_authors.author_id'
                    ],
                ],
            ],
            $joins);
        */
    }

    public function testJoinFromAtoCThroughBRequiringD()
    {
        $dataPackage = [ // Cf. http://frictionlessdata.io/specs/data-package/
            'resources' => [
                // Cf. https://frictionlessdata.io/specs/table-schema/
                [
                    'name' => 'tableA',
                    'schema' => [
                        'fields' => [
                            [
                                'name' => 'id',
                            ]
                        ]
                    ]
                ],
                [
                    'name' => 'tableB',
                    'schema' => [
                        'fields' => [
                            [
                                'name' => 'id',
                            ],
                            [
                                'name' => 'tableA_id',
                            ],
                        ]
                    ],
                    'foreignKeys' => [
                        [
                            'fields' => 'tableA_id',
                            'reference' =>  [
                                'resource' => 'tableA',
                                'fields' => 'id'
                            ]
                        ]
                    ]
                ],
                [
                    'name' => 'tableC',
                    'schema' => [
                        'fields' => [
                            [
                                'name' => 'id',
                            ],
                            [
                                'name' => 'tableB_id',
                            ],
                        ]
                    ],
                    'foreignKeys' => [
                        [
                            'fields' => 'tableB_id',
                            'reference' =>  [
                                'resource' => 'tableB',
                                'fields' => 'id'
                            ]
                        ]
                    ]
                ],
                [
                    'name' => 'tableD',
                    'schema' => [
                        'fields' => [
                            [
                                'name' => 'id',
                            ],
                            [
                                'name' => 'tableB_id',
                            ],
                        ]
                    ],
                    'foreignKeys' => [
                        [
                            'fields' => 'tableB_id',
                            'reference' =>  [
                                'resource' => 'tableB',
                                'fields' => 'id'
                            ]
                        ]
                    ]
                ]
            ]
        ];

        $joins = $this->dataPackageService->getJoinsFromTo($dataPackage, 'tableA', 'tableC');
        $this->assertEquals([
                'tableB' => [
                    'on' => ['tableA.id=tableB.tableA_id']
                ],
                'tableC' => [
                    'on' => ['tableB.id=tableC.tableB_id']
                ],
            ],
            $joins);

        $joins = $this->dataPackageService->getJoinsFromTo($dataPackage, 'tableA', 'tableC', 'tableB');
        $this->assertEquals([
                'tableB' => [
                    'on' => ['tableA.id=tableB.tableA_id']
                ],
                'tableC' => [
                    'on' => ['tableB.id=tableC.tableB_id']
                ],
            ],
            $joins);

        $joins = $this->dataPackageService->getJoinsFromTo($dataPackage, 'tableA', 'tableC', 'tableD');
        $this->assertEquals([
                'tableB' => [
                    'on' => ['tableA.id=tableB.tableA_id']
                ],
                'tableC' => [
                    'on' => ['tableB.id=tableC.tableB_id']
                ],
                'tableD' => [
                    'on' => ['tableB.id=tableD.tableB_id']
                ],
            ],
            $joins);
    }
}
