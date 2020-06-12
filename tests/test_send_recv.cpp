#include "test_send_recv.hpp"

#include "CppUnitAdditionalMacros.hpp"
#include "colza/request.hpp"

CPPUNIT_TEST_SUITE_REGISTRATION(SendRecvTest);

namespace tl = thallium;

extern colza::controller* m_controller;
extern std::shared_ptr<colza::communicator> m_comm;

void SendRecvTest::setUp() {}

void SendRecvTest::tearDown() {}

void SendRecvTest::testSendRecv() {
  MPI_Barrier(MPI_COMM_WORLD);
  int rank = m_comm->rank();
  if (rank == 0) {
    std::cout << "---test testSendRecv" << std::endl;
  }
  int size = m_comm->size();
  CPPUNIT_ASSERT(rank >= 0 && rank < size);
  CPPUNIT_ASSERT(size == 2);
  std::vector<char> data(256, 0);
  if (rank % 2 == 0) {
    for (unsigned i = 0; i < 256; i++) {
      data[i] = 'A' + (i % 26);
    }
    int ret = m_comm->send((const void*)data.data(), 256, rank + 1, 1234);
    CPPUNIT_ASSERT(ret == 0);
  } else {
    int ret = m_comm->recv((void*)data.data(), 256, rank - 1, 1234);
    CPPUNIT_ASSERT(ret == 0);
    for (unsigned i = 0; i < 256; i++) {
      CPPUNIT_ASSERT(data[i] == 'A' + (i % 26));
    }
  }
}

void SendRecvTest::testISendIRecv() {
  MPI_Barrier(MPI_COMM_WORLD);
  int rank = m_comm->rank();
  if (rank == 0) {
    std::cout << "---test testISendIRecv" << std::endl;
  }
  int size = m_comm->size();
  CPPUNIT_ASSERT(rank >= 0 && rank < size);
  CPPUNIT_ASSERT(size == 2);
  std::vector<char> data(256, 0);
  colza::request req;
  if (rank % 2 == 0) {
    for (unsigned i = 0; i < 256; i++) {
      data[i] = 'A' + (i % 26);
    }
    int ret = m_comm->isend((const void*)data.data(), 256, rank + 1, 1234, req);
    CPPUNIT_ASSERT(ret == 0);
    // wait for the isend to finish
    int finish = req.wait();
    CPPUNIT_ASSERT(finish == 0);
  } else {
    int ret = m_comm->irecv((void*)data.data(), 256, rank - 1, 1234, req);

    CPPUNIT_ASSERT(ret == 0);
    // wait for the ircv to finish
    int finish = req.wait();
    CPPUNIT_ASSERT(finish == 0);
    for (unsigned i = 0; i < 256; i++) {
      CPPUNIT_ASSERT(data[i] == 'A' + (i % 26));
    }
  }

  MPI_Barrier(MPI_COMM_WORLD);
}