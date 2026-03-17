/*
db2txt
Copyright (c) 2011 Bruno Sanches  http://code.google.com/p/dbf2txt

This software is provided 'as-is', without any express or implied warranty.
In no event will the authors be held liable for any damages arising from the use of this software.
Permission is granted to anyone to use this software for any purpose, 
including commercial applications, and to alter it and redistribute it freely, 
subject to the following restrictions:

1. The origin of this software must not be misrepresented; you must not claim that you wrote the original software. If you use this software in a product, an acknowledgment in the product documentation would be appreciated but is not required.
2. Altered source versions must be plainly marked as such, and must not be misrepresented as being the original software.
3. This notice may not be removed or altered from any source distribution.
*/


#include "dbf.hpp"

#include <algorithm>
#include <iostream>
#include <sstream>
#include <memory>
#include <vector>

#include "arrow/io/file.h"
#include "parquet/stream_writer.h"

DbfFile::DbfFile(const char *szFileName):
	m_clFile(szFileName, std::ios_base::binary | std::ios_base::in)
{
	if(!m_clFile.good())
		throw std::logic_error("Cannot open file");

	m_clFile.read(reinterpret_cast<char *>(&m_stHeader), sizeof(m_stHeader));
	size_t sz = sizeof(DbfRecord);

	const auto numRecords = m_stHeader.m_uNumRecords;
	
	for(unsigned i = 0;i < numRecords; ++i)
	{
		char end;
		m_clFile.read(&end, 1);
		if(end == 0x0D)
			break;

		//corrupted file? Abort to avoid infinite loop
		if (i == numRecords)
			break;

		m_vecRecords.push_back(DbfRecord());
		DbfRecord &record = m_vecRecords.back();

		memcpy(&record, &end, 1);
		m_clFile.read(reinterpret_cast<char *>(&record)+1, sizeof(DbfRecord)-1);
		SetField(record);
		
		m_szRowSize += record.m_uLength;
		m_szLargestFieldSize = std::max(m_szLargestFieldSize, static_cast<size_t>(record.m_uLength));
	}
	if (m_stHeader.m_iType >= 0x30 && m_stHeader.m_iType <= 0x32) {
		// only for visual foxpro
		size_t backlink = 263;
		m_clFile.seekg(backlink, std::ios_base::cur);
	}
}

void DbfFile::SetField(DbfRecord& record) {
	switch (record.chFieldType) {
	case 'C':
		fields.push_back(parquet::schema::PrimitiveNode::Make(
			record.m_archName,
			parquet::Repetition::OPTIONAL,
			parquet::Type::BYTE_ARRAY,
			parquet::ConvertedType::UTF8
		));
		break;
	case 'N':
		if (record.m_uDecimalPlaces > 0) {
			fields.push_back(parquet::schema::PrimitiveNode::Make(
				record.m_archName,
				parquet::Repetition::REQUIRED,
				parquet::Type::DOUBLE,
				parquet::ConvertedType::NONE
			));
		}
		else {
			fields.push_back(parquet::schema::PrimitiveNode::Make(
				record.m_archName,
				parquet::Repetition::REQUIRED,
				parquet::Type::INT32,
				parquet::ConvertedType::INT_32
			));
		}
		break;
	case 'I':
		fields.push_back(parquet::schema::PrimitiveNode::Make(
			record.m_archName,
			parquet::Repetition::REQUIRED,
			parquet::Type::INT32,
			parquet::ConvertedType::INT_32
		));
		break;
	case 'L':
		fields.push_back(parquet::schema::PrimitiveNode::Make(
			record.m_archName,
			parquet::Repetition::REQUIRED,
			parquet::Type::BOOLEAN,
			parquet::ConvertedType::NONE
		));
		break;
	case 'B':
		fields.push_back(parquet::schema::PrimitiveNode::Make(
			record.m_archName, 
			parquet::Repetition::REQUIRED, 
			parquet::Type::DOUBLE,
			parquet::ConvertedType::NONE
		));
		break;
	default:
		std::cout << record.chFieldType << " , " << record.m_archName << std::endl;
		break;
	}		
}
void DbfFile::To_Parquet(const char* szDestFileName)
{	
	std::shared_ptr<arrow::io::FileOutputStream> outfile;
	PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(szDestFileName));

	parquet::WriterProperties::Builder builder;

	std::shared_ptr<parquet::schema::GroupNode> schema = std::static_pointer_cast<parquet::schema::GroupNode>(
		parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));

	parquet::StreamWriter os {
	  parquet::ParquetFileWriter::Open(outfile, schema, builder.build()) 
	};

	std::vector<char> vecBuffer;
	auto buffer = std::make_unique<char[]>(m_szLargestFieldSize);
	vecBuffer.resize(m_szLargestFieldSize);

	size_t uTotalBytes = 0;
	size_t uNumRecords = 0;

	const auto recordsSize = m_vecRecords.size();

	while (!m_clFile.eof())
	{
		char deleted;
		m_clFile.read(&deleted, 1);
		if (deleted == 0x2A)
		{
			m_clFile.seekg(m_szRowSize, std::ios_base::cur);
			continue;
		}

		if (m_clFile.fail())
			break;

		if (deleted == 0x1A) //end-of-file marker
			break;				
		for (size_t i = 0; i < recordsSize; ++i)
		{
			DbfRecord& record = m_vecRecords[i];			
			//std::cout << record.m_archName << std::endl;
			m_clFile.read(&buffer[0], record.m_uLength);						
			switch (record.chFieldType)
			{
			case 'C':
				{
					std::string temp(&buffer[0], 0, record.m_uLength);
					stripUnicode(temp);						
					os << temp;
					// std::cout << temp << std::endl;
				}
				break;
			case 'N':
				{
					std::string temp(&buffer[0], 0, record.m_uLength);
					if (record.m_uDecimalPlaces > 0) {
						os << std::atof(temp.c_str());
					} 
					else {						
						os << std::atoi(temp.c_str());
					}					
				}
				break;
			case 'I':
				{					
					std::int32_t value = (static_cast<unsigned char>(buffer[3]) << 24) |
						(static_cast<unsigned char>(buffer[2]) << 16) |
						(static_cast<unsigned char>(buffer[1]) << 8) |
						static_cast<unsigned char>(buffer[0]);										
 					os << value;
				}
				break;
			case 'L':
				{
					std::string temp(&buffer[0], 0, record.m_uLength);
					bool val = ((temp == "T" ) ? true : false );
					os << val;
				}
				break;
			case 'B':
				{
					std::string temp(&buffer[0], 0, record.m_uLength);
					os << std::atof(temp.c_str());
				}
				break;
			default:
				break;
			}
			//os.write(&vecBuffer[0], record.m_uLength);

			uTotalBytes += record.m_uLength;
		}
		++uNumRecords;
		++uTotalBytes;
		os << parquet::EndRow;
		if ((uNumRecords % 10000) == 0) {
			//os << parquet::EndRowGroup;
			std::cout << "#";
		}
	}		
	std::cout <<std::endl << "Created " << uNumRecords << " records, " << uTotalBytes << " bytes.\n";
}
