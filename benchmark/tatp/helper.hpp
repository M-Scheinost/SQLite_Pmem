#ifndef SQLITE_PERFORMANCE_TATP_HELPERS_HPP
#define SQLITE_PERFORMANCE_TATP_HELPERS_HPP

#include "cxxopts.hpp"
#include <string>
#include <vector>

const char sqlite_init[] = "PRAGMA journal_mode = WAL;"\
                    "DROP TABLE IF EXISTS call_forwarding;"\
                    "DROP TABLE IF EXISTS special_facility;"\
                    "DROP TABLE IF EXISTS access_info;"\
                    "DROP TABLE IF EXISTS subscriber;"\
                    "CREATE TABLE subscriber("\
                        "s_id         INTEGER NOT NULL PRIMARY KEY,"\
                        "sub_nbr      TEXT    NOT NULL UNIQUE,"\
                        "bit_1        INTEGER,"\
                        "bit_2        INTEGER,"\
                        "bit_3        INTEGER,"\
                        "bit_4        INTEGER,"\
                        "bit_5        INTEGER,"\
                        "bit_6        INTEGER,"\
                        "bit_7        INTEGER,"\
                        "bit_8        INTEGER,"\
                        "bit_9        INTEGER,"\
                        "bit_10       INTEGER,"\
                        "hex_1        INTEGER,"\
                        "hex_2        INTEGER,"\
                        "hex_3        INTEGER,"\
                        "hex_4        INTEGER,"\
                        "hex_5        INTEGER,"\
                        "hex_6        INTEGER,"\
                        "hex_7        INTEGER,"\
                        "hex_8        INTEGER,"\
                        "hex_9        INTEGER,"\
                        "hex_10       INTEGER,"\
                        "byte2_1      INTEGER,"\
                        "byte2_2      INTEGER,"\
                        "byte2_3      INTEGER,"\
                        "byte2_4      INTEGER,"\
                        "byte2_5      INTEGER,"\
                        "byte2_6      INTEGER,"\
                        "byte2_7      INTEGER,"\
                        "byte2_8      INTEGER,"\
                        "byte2_9      INTEGER,"\
                        "byte2_10     INTEGER,"\
                        "msc_location INTEGER,"\
                        "vlr_location INTEGER"\
                    ");"\
                    "CREATE TABLE access_info                                "\
                    "(                                                       "\
                    "    s_id    INTEGER NOT NULL,                           "\
                    "    ai_type INTEGER NOT NULL,                           "\
                    "    data1   INTEGER,                                    "\
                    "    data2   INTEGER,                                    "\
                    "    data3   TEXT,                                       "\
                    "    data4   TEXT,                                       "\
                    "    PRIMARY KEY (s_id, ai_type),                        "\
                    "    FOREIGN KEY (s_id) REFERENCES subscriber (s_id)     "\
                    ");                                                      "\
                    "CREATE TABLE special_facility                           "\
                    "(                                                       "\
                    "    s_id        INTEGER NOT NULL,                       "\
                    "    sf_type     INTEGER NOT NULL,                       "\
                    "    is_active   INTEGER,                                "\
                    "    error_cntrl INTEGER,                                "\
                    "    data_a      INTEGER,                                "\
                    "    data_b      TEXT,                                   "\
                    "    PRIMARY KEY (s_id, sf_type),                        "\
                    "    FOREIGN KEY (s_id) REFERENCES subscriber (s_id)     "\
                    ");                                                      "\
                    "CREATE TABLE call_forwarding                            "\
                    "(                                                       "\
                    "    s_id       INTEGER NOT NULL,                        "\
                    "    sf_type    INTEGER NOT NULL,                        "\
                    "    start_time INTEGER NOT NULL,                        "\
                    "    end_time   INTEGER,                                 "\
                    "    numberx    TEXT,                                    "\
                    "    PRIMARY KEY (s_id, sf_type, start_time),            "\
                    "    FOREIGN KEY (s_id, sf_type)                         "\
                    "        REFERENCES special_facility (s_id, sf_type)     "\
                    ");";

const std::string prep_sub = "INSERT INTO subscriber VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
const std::string prep_ai = "INSERT INTO access_info VALUES (?,?,?,?,?,?)";
const std::string prep_sf = "INSERT INTO special_facility VALUES (?,?,?,?,?,?)";
const std::string prep_cf = "INSERT INTO call_forwarding VALUES (?,?,?,?,?)";

std::vector<std::string> tatp_transactions() {
  return {"SELECT * FROM subscriber WHERE s_id = ?;",

          "SELECT cf.numberx "
          "FROM special_facility AS sf, call_forwarding AS cf "
          "WHERE sf.s_id = ? AND sf.sf_type = ? AND sf.is_active = 1 "
          "  AND cf.s_id = sf.s_id AND cf.sf_type = sf.sf_type "
          "  AND cf.start_time <= ? AND ? < cf.end_time;",

          "SELECT data1, data2, data3, data4 "
          "FROM access_info "
          "WHERE s_id = ? AND ai_type = ?;",

          "UPDATE subscriber "
          "SET bit_1 = ? "
          "WHERE s_id = ?;",

          "UPDATE special_facility "
          "SET data_a = ? "
          "WHERE s_id = ? AND sf_type = ?;",

          "UPDATE subscriber "
          "SET vlr_location = ? "
          "WHERE sub_nbr = ?;",

          "SELECT s_id "
          "FROM subscriber "
          "WHERE sub_nbr = ?;",

          "SELECT sf_type "
          "FROM special_facility "
          "WHERE s_id = ?;",

          "INSERT INTO call_forwarding "
          "VALUES (?, ?, ?, ?, ?);",

          "DELETE FROM call_forwarding "
          "WHERE s_id = ? AND sf_type = ? AND start_time = ?;"};
}

cxxopts::Options tatp_options(const std::string &program,
                              const std::string &help_string = "") {
  cxxopts::Options options(program, help_string);
  cxxopts::OptionAdder adder = options.add_options();
  adder("load", "Load the database");
  adder("run", "Run the benchmark");
  adder("records", "Number of subscriber records",
        cxxopts::value<uint64_t>()->default_value("1000"));
  adder("clients", "Number of clients",
        cxxopts::value<size_t>()->default_value("1"));
  adder("warmup", "Warmup duration in seconds",
        cxxopts::value<size_t>()->default_value("10"));
  adder("measure", "Measure duration in seconds",
        cxxopts::value<size_t>()->default_value("60"));
  adder("help", "Print help");
  return options;
}




#endif // SQLITE_PERFORMANCE_TATP_HELPERS_HPP