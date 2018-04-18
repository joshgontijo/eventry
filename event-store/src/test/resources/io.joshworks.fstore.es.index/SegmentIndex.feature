Feature: Get range
  Scenario: Get IndexEntry within range
    Given a SegmentIndex with the following entries
      | stream  | version | position  |
      | 1       | 0       | 0         |
      | 1       | 1       | 0         |
      | 1       | 2       | 0         |
      | 1       | 3       | 0         |
      | 1       | 4       | 0         |

    When a range of stream 1, with start version 1 and end version 4
    Then 3 entries should be returned
      And First entry must be stream 1, version 1
      And Second entry must be stream 1, version 2
      And Third entry must be stream 1, version 3

  Scenario: Entry within range, but not found
    Given a SegmentIndex with the following entries
      | stream  | version | position  |
      | 1       | 0       | 0         |
      | 1       | 1       | 0         |
      | 3       | 0       | 0         |
      | 3       | 1       | 0         |

    When a range of stream 2, with start version 0 and end version 999
    Then 0 entries should be returned


  Scenario: Return the latest version of stream
    Given a SegmentIndex with the following entries
      | stream  | version | position  |
      | 1       | 0       | 0         |
      | 1       | 1       | 0         |
      | 3       | 0       | 0         |
      | 3       | 1       | 0         |

    When the latest version of stream 1 is queried
    Then I should get version 1 of stream 1