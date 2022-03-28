#include <cmath>
#include <chrono>
#include <memory>
#include <string>
#include <cstdlib>
#include <fcntl.h>
#include <unistd.h>
#include <sys/dir.h>
#include <functional>
#include <grpc++/grpc++.h>
#include "payload.grpc.pb.h"

using grpc::Status;
using grpc::Channel;
using grpc::ClientReader;
using grpc::ClientWriter;
using grpc::ClientContext;

using p2_grpc::AFS;
using p2_grpc::WriteStreamReq;
using p2_grpc::WriteReply;
using p2_grpc::ReadReply;
using p2_grpc::ReadReq;

class GRPCClient {
private:
    std::unique_ptr<AFS::Stub> stub;
    static const int BASE_RETRY = 100;
    static const int RETRY_TIME_MULTIPLIER = 100;
    static const int GRPC_REQ_FAIL_ERR_CODE = -10;

    static inline std::chrono::system_clock::time_point get_timeout_deadline(long timout_milliseconds) {
        return std::chrono::system_clock::now() + std::chrono::milliseconds(timout_milliseconds);
    }

public:
    GRPCClient();

    GRPCClient(std::shared_ptr<Channel> channel) : stub(AFS::NewStub(channel)) {}

    int SendWrite(const char *path, const std::string dataString) {
        ClientContext clientContext;
        WriteStreamReq writeStreamReq;
        WriteReply writeReply;
        std::unique_ptr<ClientWriter<WriteStreamReq>> clientWriter(stub->SendWrite(&clientContext, &writeReply));

        writeStreamReq.set_path(std::string(path));
        size_t write_size = 4096; //4kb
        writeStreamReq.set_size(write_size); //fix size
        writeStreamReq.set_buf(dataString);

        if (!clientWriter->Write(writeStreamReq)) {
            return GRPC_REQ_FAIL_ERR_CODE;
        }
        clientWriter->WritesDone();
        Status status = clientWriter->Finish();

        int return_code = writeReply.writedone() ? 0 : writeReply.responsecode();
        return status.ok() ? return_code : GRPC_REQ_FAIL_ERR_CODE;
    }

    //To-DO: Read should return string or not?
    int SendRead(const char *path){
        ClientContext clientContext;
        ReadReq readReq;
        ReadReply readReply;
        std::string readBuffer;
        readReq.set_path(std::string(path));
        std::unique_ptr<ClientReader<ReadReply>> clientReader(stub->SendRead(&clientContext, readReq));

        int return_value = 0;

        while (clientReader->Read(&readReply)) {
            readBuffer.resize(readReply.size());
            readBuffer = readReply.buf();
            if (readReply.responsecode() < 0) {
                return_value = readReply.responsecode();
                break;
            }
        }

        //Optional for Demo
        // cout<<"Received data: "<<buf;
        Status status = clientReader -> Finish();
        return status.ok() ? return_value : GRPC_REQ_FAIL_ERR_CODE;
    }
};

class MainClient {
private:
    static const int BASE_RETRY = 100;
    static const int RETRY_TIME_MULTIPLIER = 100;
    static const int GRPC_REQ_FAIL_ERR_CODE = -10;
    GRPCClient grpcClientA;
    GRPCClient grpcClientB;
    GRPCClient primary;
    GRPCClient backup;

    static inline std::chrono::system_clock::time_point get_timeout_deadline(long timout_milliseconds) {
        return std::chrono::system_clock::now() + std::chrono::milliseconds(timout_milliseconds);
    }

public:
    MainClient();

    MainClient(std::string serverAddressA , std::string serverAddressB) {
        
        GRPCClient grpcClientA(grpc::CreateChannel(serverAddressA, grpc::InsecureChannelCredentials()));
        GRPCClient grpcClientB(grpc::CreateChannel(serverAddressB, grpc::InsecureChannelCredentials()));
        primary = grpcClientA;
        backup = grpcClientB;
    }

    std::string doWrite(std::string path, std::string dataString)
    {
        //convert path to block number
        string blockNumber = getBlock(path);

        //TO DO: Switch Primary/Backup if necessary

        int returnCode = withRetry(primary.SendWrite(blockNumber, dataString), 1);
        if(returnCode<0)
        {
            returnCode = withRetry(backup.SendRead(blockNumber, dataString),1);
        }

        if(returnCode<0)
        {
            return "Write Failure";
        }

        return "Write Received";

    }

    std::string doRead(std::string path)
    {
        //convert path to block number
        string blockNumber = getBlock(path);
        v1 = rand() % 100;
        int returnCode = 0;
        if(v1%2==0)
        {
            returnCode = withRetry(primary.SendRead(blockNumber), 1);
            if(returnCode<0)
            {
                returnCode = withRetry(backup.SendRead(blockNumber),1);
            }
        }
        else
        {
            returnCode = withRetry(backup.SendRead(blockNumber), 1);
            if(returnCode<0)
            {
                returnCode = withRetry(primary.SendRead(blockNumber),1);
            }
        }

        //handle string response if any :TO-DO
        if(returnCode<0)
        {
            return "Read Failure";
        }

        return "Read Received";


    }

    int withRetry(const std::function<int()> &func, int max_tries) {
        int return_val;
        int tries = 0;
        do {
            return_val = func();
            usleep(BASE_RETRY + (tries * RETRY_TIME_MULTIPLIER));
            tries += 1;
        } while (tries < max_tries && return_val == GRPC_REQ_FAIL_ERR_CODE);
        return return_val;
    }
}

int main(int argc, char** argv) {
    std::string primaryAddr;
    std::string backupAddr;
    std::string arg_str_primary("--primary");
    std::string arg_str_backup("--backup");

    if (argc > 1) {
        std::string arg_val = argv[1];
        size_t start_pos = arg_val.find(arg_str_primary);
        if (start_pos != std::string::npos) {
            start_pos += arg_str_primary.size();
            if (arg_val[start_pos] == '=') {
                primaryAddr = arg_val.substr(start_pos + 1);
            } else {
                std::cout << "The only correct argument syntax is --primary="
                          << std::endl;
                return 0;
            }
        } else {
            std::cout << "The only acceptable argument is --primary=" << std::endl;
            return 0;
        }

        string arg_val = argv[2];
        start_pos = arg_val.find(arg_str_backup);
        if (start_pos != std::string::npos) {
            start_pos += arg_str_backup.size();
            if (arg_val[start_pos] == '=') {
                backupAddr = arg_val.substr(start_pos + 1);
            } else {
                std::cout << "The only correct argument syntax is --backup="
                          << std::endl;
                return 0;
            }
        } else {
            std::cout << "The only acceptable argument is --backupAddr=" << std::endl;
            return 0;
        }
    } else {
        primaryAddr = "localhost:50051";
        backupAddr = "localhost:50052";
    }
    

    MainClient client(std::string(primaryAddr), backupAddr);
    //Now client is running

}


