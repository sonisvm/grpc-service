#include <iostream>
#include <memory>
#include <string>
#include <ctime>

#include <grpcpp/grpcpp.h>
#include <grpc/support/log.h>

#include "store.grpc.pb.h"

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using store::ProductQuery;
using store::ProductReply;
using store::ProductInfo;
using store::Store;

class StoreClient {
 public:
  explicit StoreClient(std::shared_ptr<Channel> channel)
      : store_stub(Store::NewStub(channel)) {}


  int getProductBids(const std::string& product_name) {
    ProductQuery request;
    request.set_product_name(product_name);
    ProductReply response;

    ClientContext context;

    CompletionQueue cq;

    Status status;

    std::unique_ptr<ClientAsyncResponseReader<ProductReply> > rpc(
        store_stub->PrepareAsyncgetProducts(&context, request, &cq));

    rpc->StartCall();

    std::time_t tag = std::time(nullptr);

    rpc->Finish(&response, &status, (void*)tag);
    void* got_tag;
    bool ok = false;

    GPR_ASSERT(cq.Next(&got_tag, &ok));
    GPR_ASSERT(got_tag == (void*)tag);
    GPR_ASSERT(ok);

    if (status.ok()) {
      for (auto i = 0; i < response.products_size(); i++) {
          std::cout << "Vendor: " << response.products(i).vendor_id() << " Price: " << response.products(i).price() << "\n";
      }
      return response.products_size();
    } else {
      return 0;
    }
  }

 private:
  std::unique_ptr<Store::Stub> store_stub;
};

int main(int argc, char** argv) {
  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint (in this case,
  // localhost at port 50051). We indicate that the channel isn't authenticated
  // (use of InsecureChannelCredentials()).
  StoreClient s_client(grpc::CreateChannel(
      "localhost:50050", grpc::InsecureChannelCredentials()));
  std::string product_name("book1");
  int reply = s_client.getProductBids(product_name);  // The actual RPC call!
  std::cout << "Store received: " << reply << " products" << std::endl;

  return 0;
}
