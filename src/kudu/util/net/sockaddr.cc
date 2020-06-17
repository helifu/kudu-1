// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/util/net/sockaddr.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>

#include <cerrno>
#include <cstring>
#include <string>

#include "kudu/gutil/hash/builtin_type_hash.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/stopwatch.h"

using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {

///
/// Sockaddr
///
Sockaddr::Sockaddr() {
  set_length(0);
}

Sockaddr::~Sockaddr() {
  ASAN_UNPOISON_MEMORY_REGION(&storage_, sizeof(storage_));
}

Sockaddr::Sockaddr(const Sockaddr& other) noexcept {
  *this = other;
}

Sockaddr::Sockaddr(const struct sockaddr_in& addr)
  : Sockaddr(reinterpret_cast<const struct sockaddr&>(addr), sizeof(addr)) {
    DCHECK_EQ(AF_INET, addr.sin_family);
}

Sockaddr::Sockaddr(const struct sockaddr_in6& addr)
  : Sockaddr(reinterpret_cast<const struct sockaddr&>(addr), sizeof(addr)) {
    DCHECK_EQ(AF_INET6, addr.sin6_family);
}

Sockaddr::Sockaddr(const struct sockaddr& addr, socklen_t len) {
  set_length(len);
  memcpy(&storage_, &addr, len);
}

Sockaddr Sockaddr::Wildcard(bool ipv4) {
  // IPv4.
  if (ipv4) {
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    return Sockaddr(addr);
  }

  // IPv6.
  struct sockaddr_in6 addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin6_family = AF_INET6;
  addr.sin6_addr = IN6ADDR_ANY_INIT;
  return Sockaddr(addr);
}

Status Sockaddr::ParseString(const std::string& s, uint16_t default_port) {
  HostPort hp;
  RETURN_NOT_OK(hp.ParseString(s, default_port));
  return ParseFromNumericHostPort(hp);
}

Status Sockaddr::ParseFromNumericHostPort(const HostPort& hp) {
  sa_family_t family = strcount(hp.host(), ':') ? AF_INET6 : AF_INET;
  if (AF_INET == family) {
    storage_.in.sin_family = family;
    storage_.in.sin_port = htons(hp.port());
    if (inet_pton(AF_INET, hp.host().c_str(), &(storage_.in.sin_addr)) != 1) {
      return Status::InvalidArgument("Invalid IPv4 address", hp.host());
    }
    set_length(sizeof(struct sockaddr_in));
  } else if (AF_INET6 == family) {
    storage_.in6.sin6_family = family;
    storage_.in6.sin6_port = htons(hp.port());
    if (inet_pton(AF_INET6, hp.host().c_str(), &(storage_.in6.sin6_addr)) != 1) {
      return Status::InvalidArgument("Invalid IPv6 address", hp.host());
    }
    set_length(sizeof(struct sockaddr_in6));
  }
  return Status::OK();
}

Sockaddr& Sockaddr::operator=(const Sockaddr& other) noexcept {
  if (&other == this) return *this;
  set_length(other.len_);
  memcpy(&storage_, &other.storage_, len_);
  return *this;
}

Sockaddr& Sockaddr::operator=(const struct sockaddr_in& addr) {
  set_length(sizeof(addr));
  memcpy(&storage_, &addr, len_);
  DCHECK_EQ(family(), AF_INET);
  return *this;
}

Sockaddr& Sockaddr::operator=(const struct sockaddr_in6& addr) {
  set_length(sizeof(addr));
  memcpy(&storage_, &addr, len_);
  DCHECK_EQ(family(), AF_INET6);
  return *this;
}

bool Sockaddr::operator==(const Sockaddr& other) const {
  return BytewiseCompare(*this, other) == 0;
}

int Sockaddr::BytewiseCompare(const Sockaddr& a, const Sockaddr& b) {
  Slice a_slice(reinterpret_cast<const uint8_t*>(&a.storage_), a.len_);
  Slice b_slice(reinterpret_cast<const uint8_t*>(&b.storage_), b.len_);
  return a_slice.compare(b_slice);
}

uint32_t Sockaddr::HashCode() const {
  return HashStringThoroughly(reinterpret_cast<const char*>(&storage_), addrlen());
}

void Sockaddr::set_port(int port) {
  DCHECK_GE(port, 0);
  DCHECK_LE(port, std::numeric_limits<uint16_t>::max());
  if (AF_INET == family()) {
    storage_.in.sin_port = htons(port);
  } else {
    storage_.in6.sin6_port = htons(port);
  }
}

int Sockaddr::port() const {
  if (AF_INET == family()) {
    return ntohs(storage_.in.sin_port);
  }
  return ntohs(storage_.in6.sin6_port);
}

std::string Sockaddr::host() const {
  switch (family()) {
  case AF_INET:
    return HostPort::AddrToString(AF_INET, &(storage_.in.sin_addr.s_addr));
  case AF_INET6:
    return HostPort::AddrToString(AF_INET6, &(storage_.in6.sin6_addr.s6_addr));
  default:
    DCHECK(false) << "unexpected host() call on socket with family " << family();
    return "<unknown socket type>";
  }
}

const struct sockaddr_in& Sockaddr::ipv4_addr() const {
  DCHECK_EQ(family(), AF_INET);
  return storage_.in;
}

const struct sockaddr_in6& Sockaddr::ipv6_addr() const {
  DCHECK_EQ(family(), AF_INET6);
  return storage_.in6;
}

std::string Sockaddr::ToString() const {
  switch (family()) {
  case AF_INET:
    return Substitute("$0:$1", host(), port());
  case AF_INET6:
    return Substitute("[$0]:$1", host(), port());
  default:
    return "<invalid sockaddr>";
  }
}

bool Sockaddr::IsWildcard() const {
  if (AF_INET == family()) {
    return 0 == storage_.in.sin_addr.s_addr;
  }
  return IN6_IS_ADDR_UNSPECIFIED(&storage_.in6.sin6_addr);
}

bool Sockaddr::IsAnyLocalAddress() const {
  uint128_t addr = 0;
  if (AF_INET == family()) {
    addr = storage_.in.sin_addr.s_addr;
  } else {
    memcpy(&addr, storage_.in6.sin6_addr.s6_addr, sizeof(addr));
  }
  return HostPort::IsLoopback(family(), addr);
}

Status Sockaddr::LookupHostname(string* hostname) const {
  char host[NI_MAXHOST];
  int flags = 0;

  auto* addr = reinterpret_cast<const sockaddr*>(&storage_);
  int rc = 0;
  LOG_SLOW_EXECUTION(WARNING, 200,
                     Substitute("DNS reverse-lookup for $0", ToString())) {
    rc = getnameinfo(addr, addrlen(), host, NI_MAXHOST, nullptr, 0, flags);
  }
  if (PREDICT_FALSE(rc != 0)) {
    if (rc == EAI_SYSTEM) {
      int errno_saved = errno;
      return Status::NetworkError(Substitute("getnameinfo: $0", gai_strerror(rc)),
                                  strerror(errno_saved), errno_saved);
    }
    return Status::NetworkError("getnameinfo", gai_strerror(rc), rc);
  }
  *hostname = host;
  return Status::OK();
}

string Sockaddr::ToCommaSeparatedString(const std::vector<Sockaddr>& addrs) {
  vector<string> addrs_str;
  addrs_str.reserve(addrs.size());
  for (const Sockaddr& addr : addrs) {
    addrs_str.push_back(addr.ToString());
  }
  return JoinStrings(addrs_str, ",");
}

void Sockaddr::set_length(socklen_t len) {
  DCHECK(len == 0 || len >= sizeof(sa_family_t));
  DCHECK_LE(len, sizeof(storage_));
  len_ = len;
  ASAN_UNPOISON_MEMORY_REGION(&storage_, len_);
  ASAN_POISON_MEMORY_REGION(reinterpret_cast<uint8_t*>(&storage_) + len_,
                            sizeof(storage_) - len_);
}

} // namespace kudu
