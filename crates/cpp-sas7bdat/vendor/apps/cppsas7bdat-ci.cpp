/**
 *  \file apps/cppsas7bdat-ci.cpp
 *
 *  \brief Main file
 *
 *  \author Olivia Quinet
 */

#include <cppsas7bdat/version.hpp>
#include <cppsas7bdat/reader.hpp>
#include <cppsas7bdat/source/ifstream.hpp>
#include <cppsas7bdat/sink/print.hpp>
#include <cppsas7bdat/sink/csv.hpp>
#include <cppsas7bdat/sink/null.hpp>
#include <docopt/docopt.h>

namespace {
  static constexpr auto USAGE =
R"(SAS7BDAT file reader

     Usage:
       cppsas7bdat-ci print [--nlines=<lines>] <file>...
       cppsas7bdat-ci csv <file>...
       cppsas7bdat-ci null <file>...
       cppsas7bdat-ci (-h|--help)
       cppsas7bdat-ci (-v|--version)

     Options:
       -h --help                    Show this screen.
       -v --version                 Show version.
       -n=<lines> --nlines=<lines>  Read at most n lines
)";
}

void process_print(const std::string& _filename, long _n)
{
  cppsas7bdat::Reader reader(cppsas7bdat::datasource::ifstream(_filename.c_str()), cppsas7bdat::datasink::print(std::cout));
  while(_n != 0 && reader.read_row()) {
    if(_n > 0) --_n;
  }
}

std::string get_csv_filename(const std::string& _filename)
{
  auto ipos = _filename.rfind('.');
  if(ipos != _filename.npos) {
    return _filename.substr(0, ipos+1) + "csv";
  }
  return _filename + "csv";
}

void process_csv(const std::string& _filename)
{
  const auto csv_filename = get_csv_filename(_filename);
  //std::ofstream csv_os(csv_filename.c_str());
  //cppsas7bdat::Reader reader(cppsas7bdat::datasource::ifstream(_filename.c_str()), cppsas7bdat::datasink::csv(csv_os));
  cppsas7bdat::Reader reader(cppsas7bdat::datasource::ifstream(_filename.c_str()), cppsas7bdat::datasink::csv(_filename.c_str()));
  reader.read_all();
}

void process_null(const std::string& _filename)
{
  cppsas7bdat::Reader reader(cppsas7bdat::datasource::ifstream(_filename.c_str()), cppsas7bdat::datasink::null());
  reader.read_all();
}

int main(const int argc, char* argv[])
{
  std::string version = fmt::format("CPP SAS7BDAT file reader {}", cppsas7bdat::getVersion());
  std::map<std::string, docopt::value> args = docopt::docopt(USAGE,
							     { std::next(argv), std::next(argv, argc) },
							     true,// show help if requested
							     version);
  if(false)
    for (auto const &arg : args) {
      std::cout << arg.first << " = " << arg.second << std::endl;
    }
  if(args["print"].asBool()) {
    const auto n = args["--nlines"] ? args["--nlines"].asLong() : -1;
    const auto files = args["<file>"].asStringList();
    for(const auto& file: files) {
      process_print(file, n);
    }
  } else if(args["csv"].asBool()) {
    const auto files = args["<file>"].asStringList();
    for(const auto& file: files) {
      process_csv(file);
    }
  } else if(args["null"].asBool()) {
    const auto files = args["<file>"].asStringList();
    for(const auto& file: files) {
      process_null(file);
    }
  }
  return 0;
}
