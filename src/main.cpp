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

#include <iostream>
#include <filesystem>

#include "dbf.hpp"

using namespace std;
namespace fs = std::filesystem;

int main(int argc, const char **argv)
{
	try
	{
		/*
			string dbfPath = "C:\\workspace\\DBs\\ust\\raw\\WORKS.DBF";
			string outPath = "C:\\workspace\\DBs\\ust\\raw\\WORKS.parquet";		
		*/
		if (argc == 2) {
			string dbfPath = argv[1];
			fs::path outPath = dbfPath;
			outPath.replace_extension("parquet");
			DbfFile file(dbfPath.c_str());
			file.To_Parquet(outPath.string().c_str());			
		}			
		else
		{			
			cout << "Usage: dbfile.exe <dbf file>" << endl;
		}
	}
	catch(const exception &e)
	{
		cerr << e.what(); 
		return -1;
	}


	return 0;
}