#include <db/BufferPool.hpp>
#include <db/Database.hpp>
#include <numeric>
#include <stdexcept>
#include <algorithm>

using namespace db;

BufferPool::BufferPool()
// TODO pa1: add initializations if needed
{
  // TODO pa1: additional initialization if needed
}

BufferPool::~BufferPool() {
  // TODO pa1: flush any remaining dirty pages
  m_pagesMap.clear();
  m_recentUsed.clear();
  m_dirtyMap.clear();
  m_hashMap.clear();
}

const size_t BufferPool::pageIdToSize_t(const PageId &pid) const {

  std::hash<const db::PageId> hashPid;

  return hashPid(pid);
}

Page &BufferPool::getPage(const PageId &pid) {
  // TODO pa1: If already in buffer pool, make it the most recent page and return it

  // TODO pa1: If there are no available pages, evict the least recently used page. If it is dirty, flush it to disk

  // TODO pa1: Read the page from disk to one of the available slots, make it the most recent page
  db::Database &db = db::getDatabase();
  size_t hashPid = pageIdToSize_t(pid);
  if (contains(pid)) {
    auto itor = std::find(m_recentUsed.begin(), m_recentUsed.end(), hashPid);
    if (itor != m_recentUsed.end()) {
      m_recentUsed.erase(itor);
    }

    m_recentUsed.push_front(hashPid);
    return *m_pagesMap.find(hashPid)->second.get();
  }

  if (m_pagesMap.size() >= DEFAULT_NUM_PAGES) {
    size_t lruHashPid = m_recentUsed.back();
    PageId lruPid = m_hashMap[lruHashPid];

    auto newPid = std::move(m_pagesMap.find(lruHashPid)->second);
    m_pagesMap.insert(std::make_pair(hashPid, std::move(newPid)));
    m_hashMap.insert(std::make_pair(hashPid, pid));

    db.get(lruPid.file).writePage(getPage(lruPid), lruPid.page);
    m_pagesMap.erase(lruHashPid);

    db.get(pid.file).readPage(*m_pagesMap.find(hashPid)->second.get(), pid.page);
    if (m_dirtyMap[lruHashPid]) {
      flushPage(lruPid);
    }
    m_dirtyMap[hashPid] = std::move(false);
    m_recentUsed.pop_front();
  
    m_recentUsed.push_front(hashPid);
  
    return *m_pagesMap.find(hashPid)->second.get();
  }
  else
  {
    Page page;
    m_pagesMap.insert(std::make_pair(hashPid, std::make_unique<Page>(page)));
    m_hashMap.insert(std::make_pair(hashPid, pid));
    m_recentUsed.push_front(hashPid);
    db.get(pid.file).readPage(page, pid.page);
    m_dirtyMap[hashPid] = std::move(false);

    return *m_pagesMap.find(hashPid)->second.get();
  }

}

void BufferPool::markDirty(const PageId &pid) {
  // TODO pa1: Mark the page as dirty. Note that the page must already be in the buffer pool
  if (contains(pid)) {
    size_t hashPid = pageIdToSize_t(pid);
    m_dirtyMap[hashPid] = std::move(true);
  }
}

bool BufferPool::isDirty(const PageId &pid) const {
  // TODO pa1: Return whether the page is dirty. Note that the page must already be in the buffer pool
  if (!contains(pid)) {
    throw std::logic_error("File not exists: " + pid.file);
  }
  size_t hashPid = pageIdToSize_t(pid);
  return m_dirtyMap.at(hashPid);
}

bool BufferPool::contains(const PageId &pid) const {
  // TODO pa1: Return whether the page is in the buffer pool
  size_t hashPid = pageIdToSize_t(pid);
  return m_pagesMap.find(hashPid) != m_pagesMap.end();
}

void BufferPool::discardPage(const PageId &pid) {
  // TODO pa1: Discard the page from the buffer pool. Note that the page must already be in the buffer pool
  if (contains(pid)) {
    size_t hashPid = pageIdToSize_t(pid);
    m_pagesMap.erase(hashPid);
    m_dirtyMap.erase(hashPid);
  }
}

void BufferPool::flushPage(const PageId &pid) {
  // TODO pa1: Flush the page to disk. Note that the page must already be in the buffer pool
  if (contains(pid)) {
    db::Database &db = db::getDatabase();
    size_t hashPid = pageIdToSize_t(pid);
    auto writer = db.get(pid.file).getWrites();
    if (std::find(writer.begin(), writer.end(), pid.page) == writer.end())
    {
      db.get(pid.file).writePage(getPage(pid), pid.page);
      m_dirtyMap[hashPid] = std::move(false);
    }

  }
}

void BufferPool::flushFile(const std::string &file) {
  // TODO pa1: Flush all pages of the file to disk
  for (const auto &[hashPid, page] : m_pagesMap) {
    PageId pid = m_hashMap[hashPid];
    if (pid.file == file && isDirty(pid)) {
      flushPage(pid);
    }
  }
}
