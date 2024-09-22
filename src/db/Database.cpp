#include <db/Database.hpp>
#include <memory>
#include <stdexcept>
#include <unordered_map>

using namespace db;

BufferPool &Database::getBufferPool() { return bufferPool; }

Database &db::getDatabase() {
  static Database instance;
  return instance;
}

void Database::add(std::unique_ptr<DbFile> file) {
  // TODO pa1: add the file to the catalog. Note that the file must not exist.
  const std::string &fileName = file->getName(); 
  if (m_files.find(fileName) != m_files.end()) {
    throw std::logic_error("File already exists: " + fileName);
  }
  m_files[fileName] = std::move(file);

}

std::unique_ptr<DbFile> Database::remove(const std::string &name) {
  // TODO pa1: remove the file from the catalog. Note that the file must exist.
  auto it = m_files.find(name);
  if (it == m_files.end()) {
    throw std::logic_error("File does not exist: " + name);
  }
  bufferPool.flushFile(name);
  auto file = std::move(it->second);
  m_files.erase(it);
  return file;
}

DbFile &Database::get(const std::string &name) const {
  // TODO pa1: get the file from the catalog. Note that the file must exist.
  auto it = m_files.find(name);
  if (it == m_files.end()) {
    throw std::logic_error("File does not exist: " + name);
  }
  return *(it->second);
}
