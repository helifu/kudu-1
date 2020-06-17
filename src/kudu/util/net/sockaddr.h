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
#ifndef KUDU_UTIL_NET_SOCKADDR_H
#define KUDU_UTIL_NET_SOCKADDR_H

#include <netinet/in.h>

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "kudu/util/status.h"

namespace kudu {

class HostPort;

/// Represents a sockaddr, IPv4 and IPv6 are supported.
class Sockaddr {
 public:
  Sockaddr();
  ~Sockaddr();

  // Copy constructor.
  Sockaddr(const Sockaddr& other) noexcept;

  // Construct from an IPv4 socket address.
  explicit Sockaddr(const struct sockaddr_in &addr);

  // Construct from an IPv6 socket address.
  explicit Sockaddr(const struct sockaddr_in6& addr);

  // Common construct from an IPv4/IPv6 socket address.
  explicit Sockaddr(const struct sockaddr& addr, socklen_t len);

  // Return an IP wildcard address.
  static Sockaddr Wildcard(bool ipv4 = true);

  // Parse a string IP address of the form "A.B.C.D:port", storing the result
  // in this Sockaddr object. If no ':port' is specified, uses 'default_port'.
  // Note that this function will not handle resolving hostnames.
  //
  // Returns a bad Status if the input is malformed.
  Status ParseString(const std::string& s, uint16_t default_port);

  // Parse a HostPort instance which must contain a hostname in numeric notation
  // as described above.
  //
  // Note that this function will not handle resolving hostnames.
  Status ParseFromNumericHostPort(const HostPort& hp);

  // Assignment operators.
  Sockaddr& operator=(const Sockaddr& other) noexcept;
  Sockaddr& operator=(const struct sockaddr_in& addr);
  Sockaddr& operator=(const struct sockaddr_in6& addr);

  // Compare two addresses for equality. To be equal, the addresses must have the same
  // family and have the same bytewise representation. Two uninitialized addresses
  // are equal to each other but not to any other address.
  bool operator==(const Sockaddr& other) const;
  uint32_t HashCode() const;

  // Compare two addresses bytewise.
  //
  // Returns a negative, zero, or positive integer to indicate if 'a' is less than,
  // equal to, or greater than 'b'.
  //
  // Addresses of different families (or uninitialized addresses) can be safely compared.
  // The comparison result has no semantic meaning but is deterministic.
  static int BytewiseCompare(const Sockaddr& a, const Sockaddr& b);

  static bool BytewiseLess(const Sockaddr& a, const Sockaddr& b) {
    return BytewiseCompare(a, b) < 0;
  }

  // Returns the dotted-decimal string '1.2.3.4' of the host component of this address.
  std::string host() const;

  // Set the IP port for this address.
  // REQUIRES: is an IP address.
  void set_port(int port);

  // Get the IP port for this address.
  // REQUIRES: is an IP address.
  int port() const;

  const struct sockaddr* addr() const {
    return reinterpret_cast<const sockaddr*>(&storage_);
  }

  bool is_initialized() const {
    return len_ != 0;
  }
  socklen_t addrlen() const {
    DCHECK(is_initialized());
    return len_;
  }

  const struct sockaddr_in& ipv4_addr() const;
  const struct sockaddr_in6& ipv6_addr() const;

  sa_family_t family() const {
    DCHECK(is_initialized());
    return storage_.generic.ss_family;
  }

  // Returns the stringified address in '1.2.3.4:<port>' format.
  std::string ToString() const;

  // Returns true if the address is 0.0.0.0 or 0:0:0:0:0:0:0:0(::).
  bool IsWildcard() const;

  // Returns true if the address is 127.*.*.*
  bool IsAnyLocalAddress() const;

  // Does reverse DNS lookup of the address and stores it in hostname.
  Status LookupHostname(std::string* hostname) const;

  // Takes a vector of Sockaddr objects and returns a comma separated
  // string containing ip addresses.
  static std::string ToCommaSeparatedString(const std::vector<Sockaddr>& addrs);

  // the default auto-generated copy constructor is fine here
 private:
  // Set the length of the internal storage to 'len' and adjust ASAN poisoning
  // appropriately.
  void set_length(socklen_t len);

  // The length of valid bytes in storage_.
  //
  // For an uninitialized socket, this will be 0. Otherwise, this is guaranteed
  // to be at least sizeof(sa_family_t).
  //
  // For some address types (ipv4, ipv6) this is fixed to the size of the appropriate
  // struct. For other types (unix) this is variable-length depending on the length of
  // the path.
  socklen_t len_ = 0;

  // Internal storage. This is a tagged union based on 'generic.ss_family'.
  union {
    struct sockaddr_storage generic;
    struct sockaddr_in6 in6;
    struct sockaddr_in in;
  } storage_;
};

} // namespace kudu

// Specialize std::hash for Sockaddr
namespace std {
template<>
struct hash<kudu::Sockaddr> {
  int operator()(const kudu::Sockaddr& addr) const {
    return addr.HashCode();
  }
};
} // namespace std
#endif
