#include "threadpool.h"

#include <memory>
#include <iostream>
#include <string>
#include <thread>
#include <fstream>

#include <grpcpp/grpcpp.h>
#include <grpc/support/log.h>

#include "store.grpc.pb.h"
#include "vendor.grpc.pb.h"


using vendor::Vendor;
using vendor::BidReply;
using vendor::BidQuery;

using store::ProductQuery;
using store::ProductReply;
using store::ProductInfo;
using store::Store;

using grpc::Channel;
using grpc::CompletionQueue;
using grpc::ClientContext;
using grpc::ClientAsyncResponseReader;

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;

class Threadpool;

class StoreImpl {
 public:
  void Run(std::string server_address, int pool_size) {
    ServerBuilder builder;

    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

    builder.RegisterService(&service);

    store_request_cq = builder.AddCompletionQueue();

    store_server = builder.BuildAndStart();

    std::cout << "Server listening on " << server_address << std::endl;

    // Spawning threads to handle the requests
    Threadpool threadpool(pool_size);
    threadpool.intializeThreadpool(this);
    threadpool.waitForThreads();
  }

	void setUpConnections(){
		std::ifstream vendor_address("./vendor_addresses.txt");
		std::string vendor_server;
		while(vendor_address >> vendor_server ){
			//form a channel and create a Stub
			std::shared_ptr<Channel> channel = grpc::CreateChannel(vendor_server, grpc::InsecureChannelCredentials());
			std::unique_ptr<Vendor::Stub> vendor_stub = Vendor::NewStub(channel);
			vendor_stubs.push_back(std::move(vendor_stub));  //unique_ptr cannot be copied, have to be moved
			vendor_channels.push_back(channel);
		}
	}

	ProductReply getProductBids(std::string product_name){
		CompletionQueue vendor_response_cq;

		BidQuery query;
		query.set_product_name(product_name);

		//setting up client context, status, responses
		//ClientContext cannot be reused across rpcs
		std::vector<ClientContext*> client_contexts;

		//we need a different status for each call
		std::vector<Status> statuses;
		std::vector<BidReply> responses;
		std::vector<std::unique_ptr<ClientAsyncResponseReader<BidReply>>> response_readers;
    ProductInfo* vendor_response;
    ProductReply reply;

		for (auto i = 0; i < vendor_stubs.size(); i++) {
			ClientContext* client_context = new ClientContext();
			client_contexts.push_back(client_context);

			Status status;
			statuses.push_back(status);

			BidReply response;
			responses.push_back(response);

			response_readers.push_back(std::move(vendor_stubs[i]->PrepareAsyncgetProductBid(client_context, query, &vendor_response_cq)));
			response_readers[i]->StartCall();

		}

		int responses_rcvd = 0;
		while (responses_rcvd < vendor_stubs.size()) {
			response_readers[responses_rcvd]->Finish(&responses[responses_rcvd], &statuses[responses_rcvd], (void *)responses_rcvd);

			void* response_tag = (void *) responses_rcvd;
			bool ok = false;

			GPR_ASSERT(vendor_response_cq.Next(&response_tag, &ok));
			GPR_ASSERT(ok);

			responses_rcvd++;
		}

		for (int i = 0; i < responses.size(); i++) {
      vendor_response = reply.add_products();
      vendor_response->set_vendor_id(responses[i].vendor_id());
      vendor_response->set_price(responses[i].price());
		}

    return reply;
	}


  void handleRequests() {
    // Spawn a new CallData instance to serve new clients.
    new RequestHandler(&service, store_request_cq.get(), this);
    void* tag;  // uniquely identifies a request.
    bool ok;
    while (true) {
      GPR_ASSERT(store_request_cq->Next(&tag, &ok));
      GPR_ASSERT(ok);
      static_cast<RequestHandler*>(tag)->processRequest();
    }
  }

 private:
  class RequestHandler {
   public:
    RequestHandler(Store::AsyncService* service, ServerCompletionQueue* cq, StoreImpl* store)
        : service(service), store_request_cq(cq), response_writer(&store_server_context), finish(false), store(store) {
        service->RequestgetProducts(&store_server_context, &request, &response_writer, store_request_cq, store_request_cq,
                                    this);
    }

    void processRequest() {
      std::vector<ProductInfo> responses;
      if (!finish) {
        new RequestHandler(service, store_request_cq, store);

        reply = store->getProductBids(request.product_name());
        finish = true;
        response_writer.Finish(reply, Status::OK, this);

      } else {
        delete this;
      }
    }

   private:
    Store::AsyncService* service;
    ServerCompletionQueue* store_request_cq;
    ServerContext store_server_context;
    ProductQuery request;
    ProductReply reply;
    StoreImpl* store;

    ServerAsyncResponseWriter<ProductReply> response_writer;
    bool finish;
  };

  std::unique_ptr<ServerCompletionQueue> store_request_cq;
  Store::AsyncService service;
  std::unique_ptr<Server> store_server;
  std::vector<std::unique_ptr<Vendor::Stub>> vendor_stubs;
  std::vector<std::shared_ptr<Channel>> vendor_channels;
};

Threadpool::Threadpool(int pool_size): pool_size(pool_size){}

void Threadpool::intializeThreadpool(StoreImpl *store){
  for (auto i = 0; i < 5; i++) {
    std::thread t(&StoreImpl::handleRequests, store);
    this->threads.push_back(std::move(t));
  }
}

void Threadpool::waitForThreads(){
  for (auto i = 0; i < 5; i++) {
    this->threads[i].join();
  }
}

int main(int argc, char** argv) {

  std::string server_address = argv[1];
  int pool_size = atoi(argv[2]);

  StoreImpl storeObj;
  storeObj.setUpConnections();
  storeObj.Run(server_address, pool_size);

  return 0;
}
