/// @file
/// @brief schema and dut classes for MySQL
/// @company www.greatopensource.com

#ifndef MYSQL_HH
#define MYSQL_HH

extern "C"  {
#include <mysql/mysql.h>
}

#include <sstream>
#include <string>

#include "schema.hh"
#include "relmodel.hh"
#include "dut.hh"
#include "log.hh"

struct mysql_connection {
  MYSQL mysql;
  char *zErrMsg = 0;
  int rc;
  void q(const char *query);
  mysql_connection(std::string &conninfo);
  int parse_connection_string(std::string &conninfo);
  ~mysql_connection();
  const char* host = "127.0.0.1";
  int port = 3306;
  const char* dbname ="test";
  const char* username = "root";
  const char* password = "";
  char* conninfo_buf = NULL;
};

struct schema_mysql : schema, mysql_connection {
  schema_mysql(std::string &conninfo, bool no_catalog);
  virtual std::string quote_name(const std::string &id) {
    std::stringstream ss;
    ss << "`" << id << "`";
    return ss.str();
  }
};

struct dut_mysql : dut_base {
  virtual void test(const std::string &stmt);
  dut_mysql(std::string &conninfo, bool log=false);
  void command(const char* sql);
  string conninfo;
  uint64_t queries=0;
  uint64_t failed=0;
  struct error_dumper err_log;
  bool log;
};

#endif

