/**
 * Sirius SuperContract V2 — Milestone 3 Native C++ On-Chain Transaction Bridge (`catapult.tools.txbridge`)
 *
 * Uses the official Sirius Catapult C++ SDK (`TransferBuilder`, `AggregateTransactionBuilder`,
 * `AggregateCosignatureAppender`, `TransactionExtensions`, `ionet::Connect`, `net::VerifyServer`)
 * to construct, multi-cosign (3/3 BFT validators), and broadcast real on-chain transactions
 * directly to the live 3-node C++ cluster (`node1:7900`, `node2:7910`, `node3:7920`).
 *
 * Modes:
 *   1. `trigger`:
 *      catapult.tools.txbridge trigger <generationHash> <nodePort> <nodeBootPubKey> \
 *        <callerPrivKey> <contractAddress> <mosaicIdHex> <amount> <messageHex>
 *
 *   2. `settle`:
 *      catapult.tools.txbridge settle <generationHash> <nodePort> <nodeBootPubKey> \
 *        <contractPrivKey> <cosigner1Priv> <cosigner2Priv> <cosigner3Priv> \
 *        <mosaicIdHex> <payoutSpec1: recipientPubKeyHex:amount:msgHex> [payoutSpec2...]
 */

#include "catapult/builders/AggregateTransactionBuilder.h"
#include "catapult/builders/TransferBuilder.h"
#include "catapult/builders/MosaicDefinitionBuilder.h"
#include "catapult/builders/MosaicSupplyChangeBuilder.h"
#include "catapult/model/MosaicProperty.h"
#include "catapult/model/TransferTransaction.h"
#include "catapult/state/AccountStateSerializer.h"
#include "catapult/io/BlockElementSerializer.h"
#include "catapult/io/BufferInputStreamAdapter.h"
#include <iomanip>
#include <fstream>
#include <boost/filesystem.hpp>
#include <rocksdb/db.h>
#include "catapult/crypto/KeyPair.h"
#include "catapult/crypto/KeyUtils.h"
#include "catapult/extensions/ConversionExtensions.h"
#include "catapult/extensions/TransactionExtensions.h"
#include "catapult/ionet/Node.h"
#include "catapult/ionet/PacketPayloadFactory.h"
#include "catapult/ionet/PacketSocket.h"
#include "catapult/model/Address.h"
#include "catapult/model/EntityHasher.h"
#include "catapult/net/VerifyPeer.h"
#include "catapult/utils/FileSize.h"
#include "catapult/utils/HexFormatter.h"
#include "catapult/utils/HexParser.h"
#include "catapult/utils/NetworkTime.h"
#include <boost/asio.hpp>
#include <iostream>
#include <random>
#include <sstream>
#include <vector>

using namespace catapult;

namespace {
	uint8_t RandomByte() {
		static std::random_device rd;
		static std::mt19937_64 gen(rd());
		return static_cast<uint8_t>(gen());
	}

	crypto::KeyPair GenerateEphemeralKeyPair() {
		return crypto::KeyPair::FromPrivate(crypto::PrivateKey::Generate(RandomByte));
	}

	std::vector<uint8_t> ParseHexVector(const std::string& hexStr) {
		if (hexStr.empty() || hexStr == "-")
			return {};
		if (hexStr.size() % 2 != 0)
			throw std::invalid_argument("Malformed hex string: odd character length (" + std::to_string(hexStr.size()) + ")");
		std::vector<uint8_t> out(hexStr.size() / 2);
		utils::ParseHexStringIntoContainer(hexStr.data(), hexStr.size(), out);
		return out;
	}

	std::string ToHexString(const uint8_t* data, size_t size) {
		static const char* hex = "0123456789ABCDEF";
		std::string out;
		out.reserve(size * 2);
		for (size_t i = 0; i < size; ++i) {
			out.push_back(hex[(data[i] >> 4) & 0xF]);
			out.push_back(hex[data[i] & 0xF]);
		}
		return out;
	}

	uint64_t ParseHexU64(const std::string& hexStr) {
		return std::stoull(hexStr, nullptr, 16);
	}

	bool PushTransactionToNode(
			const std::string& host,
			unsigned short port,
			const Key& serverPublicKey,
			const std::shared_ptr<model::Transaction>& pTransaction) {
		boost::asio::io_context ioContext;
		auto pClientKeyPair = std::make_shared<crypto::KeyPair>(GenerateEphemeralKeyPair());

		net::VerifiedPeerInfo serverPeerInfo;
		serverPeerInfo.PublicKey = serverPublicKey;
		serverPeerInfo.SecurityMode = ionet::ConnectionSecurityMode::None;

		ionet::NodeEndpoint endpoint{ host, port };
		ionet::PacketSocketOptions socketOptions;
		socketOptions.WorkingBufferSize = utils::FileSize::FromKilobytes(512).bytes();
		socketOptions.WorkingBufferSensitivity = 100;
		socketOptions.MaxPacketDataSize = utils::FileSize::FromMegabytes(10).bytes();

		bool pushSucceeded = false;
		auto pTimer = std::make_shared<boost::asio::steady_timer>(ioContext);
		pTimer->expires_after(std::chrono::seconds(5));
		pTimer->async_wait([&ioContext](const boost::system::error_code& ec) {
			if (!ec) {
				ioContext.stop();
			}
		});

		auto cancel = ionet::Connect(
				ioContext,
				socketOptions,
				endpoint,
				[&ioContext, &pushSucceeded, pTimer, pTransaction, serverPeerInfo, pClientKeyPair](auto connectResult, const std::shared_ptr<ionet::PacketSocket>& pSocket) {
					if (ionet::ConnectResult::Connected != connectResult || !pSocket) {
						pTimer->cancel();
						ioContext.stop();
						return;
					}
					net::VerifyServer(pSocket, serverPeerInfo, *pClientKeyPair, [pSocket, pTransaction, pTimer, pClientKeyPair, &ioContext, &pushSucceeded](auto verifyResult, const auto&) {
						if (net::VerifyResult::Success != verifyResult) {
							pSocket->close();
							pTimer->cancel();
							ioContext.stop();
							return;
						}
						auto payload = ionet::PacketPayloadFactory::FromEntity(
								ionet::PacketType::Push_Transactions,
								pTransaction);
						pSocket->write(payload, [pSocket, pTimer, &ioContext, &pushSucceeded](auto writeCode) {
							pushSucceeded = (ionet::SocketOperationCode::Success == writeCode);
							pTimer->expires_after(std::chrono::milliseconds(500));
							pTimer->async_wait([pSocket, pTimer, &ioContext](const auto&) {
								pSocket->close();
								ioContext.stop();
							});
						});
					});
				});

		ioContext.run();
		return pushSucceeded;
	}
}

int main(int argc, const char** argv) {
	try {
		if (argc < 2) {
			std::cerr << "Usage: catapult.tools.txbridge <trigger|settle> ...\n";
			return 1;
		}

	std::string mode = argv[1];
	auto networkId = model::NetworkIdentifier::Public;

	if (mode == "trigger") {
		if (argc < 10) {
			std::cerr << "Usage: trigger <genHash> <port> <nodeBootPub> <callerPriv> <contractAddr> <mosaicHex> <amount> <msgHex>\n";
			return 1;
		}
		auto generationHash = utils::ParseByteArray<GenerationHash>(argv[2]);
		unsigned short port = static_cast<unsigned short>(std::stoi(argv[3]));
		auto nodeBootPub = utils::ParseByteArray<Key>(argv[4]);
		auto callerSigner = crypto::KeyPair::FromString(argv[5]);
		auto recipientAddr = model::StringToAddress(argv[6]);
		auto mosaicId = UnresolvedMosaicId(ParseHexU64(argv[7]));
		auto amount = Amount(std::stoull(argv[8]));
		auto msgBytes = ParseHexVector(argv[9]);

		builders::TransferBuilder builder(networkId, callerSigner.publicKey());
		builder.setRecipient(extensions::CopyToUnresolvedAddress(recipientAddr));
		builder.addMosaic({ mosaicId, amount });
		if (!msgBytes.empty()) {
			builder.setMessage({ msgBytes.data(), msgBytes.size() });
		}

		auto pTxUnique = builder.build();
		pTxUnique->Deadline = Timestamp(utils::NetworkTime().unwrap() + 60 * 60 * 1000);
		pTxUnique->MaxFee = Amount(50'000'000);
		extensions::TransactionExtensions(generationHash).sign(callerSigner, *pTxUnique);

		auto txHash = model::CalculateHash(*pTxUnique, generationHash);
		std::shared_ptr<model::Transaction> pTx(std::move(pTxUnique));

		bool ok = PushTransactionToNode("127.0.0.1", port, nodeBootPub, pTx);
		std::cout << "{\"ok\":" << (ok ? "true" : "false")
				  << ",\"txHash\":\"" << ToHexString(txHash.data(), txHash.size())
				  << "\",\"size\":" << pTx->Size << "}" << std::endl;
		return ok ? 0 : 2;
	}

	if (mode == "create_mosaic") {
		if (argc < 9) {
			std::cerr << "Usage: create_mosaic <genHash> <port> <nodeBootPub> <ownerPriv> <nonceInt> <divisibility> <supply>\n";
			return 1;
		}
		auto generationHash = utils::ParseByteArray<GenerationHash>(argv[2]);
		unsigned short port = static_cast<unsigned short>(std::stoi(argv[3]));
		auto nodeBootPub = utils::ParseByteArray<Key>(argv[4]);
		auto ownerSigner = crypto::KeyPair::FromString(argv[5]);
		uint32_t nonceVal = static_cast<uint32_t>(std::stoul(argv[6]));
		uint8_t divisibility = static_cast<uint8_t>(std::stoi(argv[7]));
		uint64_t supply = std::stoull(argv[8]);

		MosaicNonce nonce(nonceVal);

		builders::MosaicDefinitionBuilder defBuilder(networkId, ownerSigner.publicKey());
		defBuilder.setMosaicNonce(nonce);
		defBuilder.setDivisibility(divisibility);
		defBuilder.setFlags(model::MosaicFlags::Transferable | model::MosaicFlags::Supply_Mutable);
		auto pDefEmbedded = defBuilder.buildEmbedded();
		auto mosaicId = pDefEmbedded->MosaicId;

		builders::MosaicSupplyChangeBuilder supplyBuilder(networkId, ownerSigner.publicKey());
		supplyBuilder.setMosaicId(UnresolvedMosaicId(mosaicId.unwrap()));
		supplyBuilder.setDirection(model::MosaicSupplyChangeDirection::Increase);
		supplyBuilder.setDelta(Amount(supply));
		auto pSupplyEmbedded = supplyBuilder.buildEmbedded();

		builders::AggregateTransactionBuilder aggBuilder(networkId, ownerSigner.publicKey());
		aggBuilder.addTransaction(std::move(pDefEmbedded));
		aggBuilder.addTransaction(std::move(pSupplyEmbedded));

		auto pAgg = aggBuilder.build();
		pAgg->Type = model::Entity_Type_Aggregate_Complete;
		pAgg->Deadline = Timestamp(utils::NetworkTime().unwrap() + 60 * 60 * 1000);
		pAgg->MaxFee = Amount(100'000'000);

		extensions::TransactionExtensions(generationHash).sign(ownerSigner, *pAgg);
		auto txHash = model::CalculateHash(*pAgg, generationHash);
		std::shared_ptr<model::Transaction> pTx(std::move(pAgg));

		bool ok = PushTransactionToNode("127.0.0.1", port, nodeBootPub, pTx);

		std::stringstream hexSs;
		hexSs << std::hex << std::uppercase << std::setfill('0') << std::setw(16) << mosaicId.unwrap();
		std::cout << "{\"ok\":" << (ok ? "true" : "false")
				  << ",\"txHash\":\"" << ToHexString(txHash.data(), txHash.size())
				  << "\",\"mosaicIdHex\":\"" << hexSs.str() << "\""
				  << ",\"mosaicIdDec\":\"" << std::dec << mosaicId.unwrap() << "\""
				  << ",\"divisibility\":" << static_cast<int>(divisibility)
				  << "}" << std::endl;
		return ok ? 0 : 2;
	}

	if (mode == "settle") {
		if (argc < 10) {
			std::cerr << "Usage: settle <genHash> <port> <nodeBootPub> <contractPriv> <cosigner1Priv> <cosigner2Priv> <cosigner3Priv> [mosaicHex] <poexMsgHex> [recipientPubHex:mosaicHex:amount:msgHex...]\n";
			return 1;
		}
		auto generationHash = utils::ParseByteArray<GenerationHash>(argv[2]);
		unsigned short port = static_cast<unsigned short>(std::stoi(argv[3]));
		auto nodeBootPub = utils::ParseByteArray<Key>(argv[4]);
		auto contractSigner = crypto::KeyPair::FromString(argv[5]);
		auto cosigner1 = crypto::KeyPair::FromString(argv[6]);
		auto cosigner2 = crypto::KeyPair::FromString(argv[7]);
		auto cosigner3 = crypto::KeyPair::FromString(argv[8]);

		// Detect if argv[9] is a fallback mosaicId (old syntax) or poexMsgHex (new syntax)
		int specStartIndex = 10;
		std::vector<uint8_t> poexMsgBytes;
		UnresolvedMosaicId defaultMosaicId(0);

		if (argc >= 11 && std::string(argv[9]).length() == 16 && std::string(argv[10]).find(':') == std::string::npos) {
			defaultMosaicId = UnresolvedMosaicId(ParseHexU64(argv[9]));
			poexMsgBytes = ParseHexVector(argv[10]);
			specStartIndex = 11;
		} else {
			poexMsgBytes = ParseHexVector(argv[9]);
			specStartIndex = 10;
		}

		builders::AggregateTransactionBuilder aggBuilder(networkId, contractSigner.publicKey());

		// 1. Add all embedded TransferTransaction payouts emitted by the WASM SuperContract
		for (int i = specStartIndex; i < argc; ++i) {
			std::string spec = argv[i];
			auto p1 = spec.find(':');
			if (p1 == std::string::npos)
				continue;
			auto p2 = spec.find(':', p1 + 1);
			if (p2 == std::string::npos)
				continue;
			auto p3 = spec.find(':', p2 + 1);

			UnresolvedMosaicId transferMosaicId = defaultMosaicId;
			Key recipientPub{};
			Amount amount(0);
			std::vector<uint8_t> msgBytes;

			if (p3 != std::string::npos) {
				// New per-transfer mosaic syntax: recipientPubHex:mosaicHex:amount:msgHex
				recipientPub = utils::ParseByteArray<Key>(spec.substr(0, p1));
				transferMosaicId = UnresolvedMosaicId(ParseHexU64(spec.substr(p1 + 1, p2 - p1 - 1)));
				amount = Amount(std::stoull(spec.substr(p2 + 1, p3 - p2 - 1)));
				msgBytes = ParseHexVector(spec.substr(p3 + 1));
			} else {
				// Legacy syntax: recipientPubHex:amount:msgHex
				recipientPub = utils::ParseByteArray<Key>(spec.substr(0, p1));
				amount = Amount(std::stoull(spec.substr(p1 + 1, p2 - p1 - 1)));
				msgBytes = ParseHexVector(spec.substr(p2 + 1));
			}

			auto recipientAddr = model::PublicKeyToAddress(recipientPub, networkId);
			builders::TransferBuilder txBuilder(networkId, contractSigner.publicKey());
			txBuilder.setRecipient(extensions::CopyToUnresolvedAddress(recipientAddr));
			txBuilder.addMosaic({ transferMosaicId, amount });
			if (!msgBytes.empty()) {
				txBuilder.setMessage({ msgBytes.data(), msgBytes.size() });
			}
			aggBuilder.addTransaction(txBuilder.buildEmbedded());
		}

		// 2. Add 3 embedded Validator Executor Attestations (requiring 3/3 BFT Cosignatures from node1, node2, node3)
		const crypto::KeyPair* validators[3] = { &cosigner1, &cosigner2, &cosigner3 };
		auto contractAddr = model::PublicKeyToAddress(contractSigner.publicKey(), networkId);
		for (int v = 0; v < 3; ++v) {
			builders::TransferBuilder attBuilder(networkId, validators[v]->publicKey());
			attBuilder.setRecipient(extensions::CopyToUnresolvedAddress(contractAddr));
			if (!poexMsgBytes.empty()) {
				attBuilder.setMessage({ poexMsgBytes.data(), poexMsgBytes.size() });
			}
			aggBuilder.addTransaction(attBuilder.buildEmbedded());
		}

		auto pAggUnsigned = aggBuilder.build();
		pAggUnsigned->Type = model::Entity_Type_Aggregate_Complete;
		pAggUnsigned->Deadline = Timestamp(utils::NetworkTime().unwrap() + 60 * 60 * 1000);
		pAggUnsigned->MaxFee = Amount(500'000'000);

		// Sign Aggregate Header with Contract Account
		extensions::TransactionExtensions(generationHash).sign(contractSigner, *pAggUnsigned);

		// Append 3/3 Validator BFT Cosignatures (node1, node2, node3)
		builders::AggregateCosignatureAppender appender(generationHash, std::move(pAggUnsigned));
		appender.cosign(cosigner1);
		appender.cosign(cosigner2);
		appender.cosign(cosigner3);

		auto pAggFinal = appender.build();
		auto headerSize = model::VerifiableEntity::Header_Size;
		RawBuffer aggBuffer{
			reinterpret_cast<const uint8_t*>(pAggFinal.get()) + headerSize,
			sizeof(model::AggregateTransaction) - headerSize + pAggFinal->PayloadSize
		};
		auto txHash = model::CalculateHash(*pAggFinal, generationHash, aggBuffer);
		std::shared_ptr<model::Transaction> pTx(std::move(pAggFinal));

		bool ok = PushTransactionToNode("127.0.0.1", port, nodeBootPub, pTx);
		std::cout << "{\"ok\":" << (ok ? "true" : "false")
				  << ",\"txHash\":\"" << ToHexString(txHash.data(), txHash.size())
				  << "\",\"size\":" << pTx->Size
				  << ",\"cosigners\":3}" << std::endl;
		return ok ? 0 : 2;
	}

	if (mode == "query_balance") {
		if (argc < 4) {
			std::cerr << "Usage: query_balance <accountPubHex> <mosaicHex> [dbPath]\n";
			return 1;
		}
		auto pubKey = utils::ParseByteArray<Key>(argv[2]);
		uint64_t mosaicIdVal = ParseHexU64(argv[3]);
		std::string dbPath = argc >= 5 ? argv[4] : "cluster/node1/data/statedb/AccountStateCache";

		auto addr = model::PublicKeyToAddress(pubKey, networkId);

		rocksdb::DB* db = nullptr;
		rocksdb::Options options;
		options.create_if_missing = false;
		rocksdb::Status status = rocksdb::DB::OpenForReadOnly(options, dbPath, &db);
		if (!status.ok()) {
			std::cout << "{\"ok\":false,\"error\":\"Failed to open statedb: " << status.ToString() << "\"}" << std::endl;
			return 1;
		}

		std::string val;
		rocksdb::Slice keySlice(reinterpret_cast<const char*>(addr.data()), addr.size());
		status = db->Get(rocksdb::ReadOptions(), keySlice, &val);
		if (!status.ok()) {
			delete db;
			std::cout << "{\"ok\":true,\"address\":\"" << model::AddressToString(addr) << "\",\"amount\":0}" << std::endl;
			return 0;
		}

		std::vector<uint8_t> valBytes(val.begin(), val.end());
		io::BufferInputStreamAdapter<std::vector<uint8_t>> stream(valBytes);
		auto accountState = state::AccountStateSerializer::Load(stream);
		Amount balance = accountState.Balances.get(MosaicId(mosaicIdVal));
		delete db;

		std::cout << "{\"ok\":true,\"address\":\"" << model::AddressToString(addr)
				  << "\",\"mosaicIdHex\":\"" << argv[3]
				  << "\",\"amount\":" << balance.unwrap() << "}" << std::endl;
		return 0;
	}

	if (mode == "query_tx") {
		if (argc < 3) {
			std::cerr << "Usage: query_tx <txHashHex> [dataDir]\n";
			return 1;
		}
		std::string txHashHex = argv[2];
		std::string dataDir = argc >= 4 ? argv[3] : "cluster/node1/data";

		if (txHashHex.size() != 64) {
			std::cout << "{\"ok\":false,\"error\":\"Invalid txHash length: expected 64 hex characters\"}" << std::endl;
			return 1;
		}

		auto targetHash = utils::ParseByteArray<Hash256>(txHashHex);

		std::string idxPath = dataDir + "/00000/blocks.idx";
		std::string datPath = dataDir + "/00000/blocks.dat";

		if (!boost::filesystem::exists(idxPath) || !boost::filesystem::exists(datPath)) {
			std::cout << "{\"ok\":false,\"error\":\"Block storage files not found in " << dataDir << "\"}" << std::endl;
			return 1;
		}

		uintmax_t idxFileSize = boost::filesystem::file_size(idxPath);
		uint32_t numIndexedBlocks = static_cast<uint32_t>(idxFileSize / 16);
		if (numIndexedBlocks <= 1) {
			std::cout << "{\"ok\":true,\"found\":false,\"error\":\"No blocks in storage\"}" << std::endl;
			return 0;
		}

		std::ifstream idxStream(idxPath, std::ios::binary);
		std::ifstream datStream(datPath, std::ios::binary);

		bool found = false;
		std::vector<uint8_t> matchedBlockBuffer;
		uint32_t matchedTxIndex = 0;

		// Scan backwards from newest block to block 1
		for (int64_t h = numIndexedBlocks - 1; h >= 1; --h) {
			idxStream.seekg(h * 16);
			uint32_t blockOffset = 0, blockSize = 0, stmtOffset = 0, stmtSize = 0;
			idxStream.read(reinterpret_cast<char*>(&blockOffset), 4);
			idxStream.read(reinterpret_cast<char*>(&blockSize), 4);
			idxStream.read(reinterpret_cast<char*>(&stmtOffset), 4);
			idxStream.read(reinterpret_cast<char*>(&stmtSize), 4);

			if (blockSize == 0)
				continue;

			datStream.seekg(blockOffset);
			uint32_t blockPayloadSize = 0;
			datStream.read(reinterpret_cast<char*>(&blockPayloadSize), 4);

			// Seek to metadata after block payload:
			// (blockOffset + blockPayloadSize + 32 (EntityHash) + 32 (GenerationHash))
			datStream.seekg(blockOffset + blockPayloadSize + 64);
			uint32_t numTx = 0;
			datStream.read(reinterpret_cast<char*>(&numTx), 4);
			if (numTx == 0)
				continue;

			// Read transaction hashes (numTx * 64 bytes)
			std::vector<uint8_t> hashesBuf(numTx * 64);
			datStream.read(reinterpret_cast<char*>(hashesBuf.data()), hashesBuf.size());

			for (uint32_t t = 0; t < numTx; ++t) {
				if (std::memcmp(hashesBuf.data() + t * 64, targetHash.data(), 32) == 0) {
					found = true;
					matchedTxIndex = t;

					matchedBlockBuffer.resize(blockSize);
					datStream.seekg(blockOffset);
					datStream.read(reinterpret_cast<char*>(matchedBlockBuffer.data()), blockSize);
					break;
				}
			}
			if (found)
				break;
		}

		if (!found) {
			std::cout << "{\"ok\":true,\"found\":false,\"error\":\"Transaction not found on chain\"}" << std::endl;
			return 0;
		}

		io::BufferInputStreamAdapter<std::vector<uint8_t>> streamAdapter(matchedBlockBuffer);
		auto pBlockElement = io::ReadBlockElement(streamAdapter);

		if (!pBlockElement || matchedTxIndex >= pBlockElement->Transactions.size()) {
			std::cout << "{\"ok\":false,\"error\":\"Failed to parse block element\"}" << std::endl;
			return 1;
		}

		const auto& tx = pBlockElement->Transactions[matchedTxIndex].Transaction;
		std::string senderHex = ToHexString(tx.Signer.data(), tx.Signer.size());

		std::stringstream jsonOut;
		jsonOut << "{\"ok\":true,\"found\":true";
		jsonOut << ",\"height\":" << pBlockElement->Block.Height.unwrap();
		jsonOut << ",\"txIndex\":" << matchedTxIndex;
		jsonOut << ",\"sender\":\"" << senderHex << "\"";
		jsonOut << ",\"type\":\"" << (tx.Type == model::Entity_Type_Transfer ? "Transfer" : "Other") << "\"";
		jsonOut << ",\"typeHex\":\"" << std::hex << std::uppercase << std::setfill('0') << std::setw(4) << static_cast<uint16_t>(tx.Type) << "\"";

		if (tx.Type == model::Entity_Type_Transfer) {
			const auto& transfer = reinterpret_cast<const model::TransferTransaction&>(tx);
			auto recipientAddr = extensions::CopyToAddress(transfer.Recipient);
			std::string recipientAddrStr = model::AddressToString(recipientAddr);
			std::string recipientHex = ToHexString(transfer.Recipient.data(), transfer.Recipient.size());

			jsonOut << ",\"recipient\":\"" << recipientAddrStr << "\"";
			jsonOut << ",\"recipientHex\":\"" << recipientHex << "\"";
			jsonOut << ",\"mosaics\":[";
			const auto* pMosaics = transfer.MosaicsPtr();
			for (uint8_t m = 0; m < transfer.MosaicsCount; ++m) {
				if (m > 0) jsonOut << ",";
				std::stringstream mHex;
				mHex << std::hex << std::uppercase << std::setfill('0') << std::setw(16) << pMosaics[m].MosaicId.unwrap();
				jsonOut << "{\"mosaicIdHex\":\"" << mHex.str() << "\""
						<< ",\"mosaicIdDec\":\"" << std::dec << pMosaics[m].MosaicId.unwrap() << "\""
						<< ",\"amount\":\"" << pMosaics[m].Amount.unwrap() << "\"}";
			}
			jsonOut << "]";
		} else {
			jsonOut << ",\"recipient\":null,\"mosaics\":[]";
		}
		jsonOut << "}";
		std::cout << jsonOut.str() << std::endl;
		return 0;
	}

	std::cerr << "Unknown mode: " << mode << "\n";
	return 1;
	} catch (const std::exception& e) {
		std::cout << "{\"ok\":false,\"error\":\"" << e.what() << "\"}" << std::endl;
		return 1;
	}
}
