from typing import Literal
from .types import CatalogFilter, CatalogFilterPayload, Storage
import pandas as pd
import requests
import os
import re
import json

CHUNK_SIZE = 1 * 1024 * 1024
class LakehouseClient:
     
    def __init__(self, lakehouse_url: str) -> None:

        pattern = re.compile(r'^(?:https?://)?(.+)$')
        match = pattern.match(lakehouse_url)
        domain = match.group(1)
        
        normalized_url = f'http://{domain}'

        self.__lakehouse_url = normalized_url
        self.__user_id = None
        self.__user_role = None
        self.__user_email = None
        self.__access_token = None
        self.__access_token_type = None
        self.__file_load_path = "./"

    # utlities
    def __file_chunk_generator(self, file_path, chunk_size=1*1024*1024):
        with open(file_path, "rb") as file:
            while chunk := file.read(chunk_size):
                yield chunk

    def __format_size(self, bytes: int):
        kb = bytes / 1024
        if kb < 1024:
            return f"{kb:.2f} KB"
        else:
            mb = kb / 1024
            return f"{mb:.2f} MB"

    def __parse_query_args(self, args: list[str]) -> list[tuple]:
        pattern = re.compile(r"^\s*([a-zA-Z_][a-zA-Z0-9_]+)\s*(=|!=|>=|<=|>|<|\*)\s*(.+?)\s*$")

        parsed_args = []

        for arg in args:
            if not isinstance(arg, str):
                raise TypeError(f"Arguments must be a string, got: {type(arg).__name__}")
            
            match = pattern.match(arg)
            if not match:
                raise ValueError(f"Invalid format: {arg!r}. Expected format: (KEY)(OPERATOR)(VALUE). Ex: 'collection_name=lakehouse'")
            
            key, op, value = match.groups()
            parsed_args.append((key, op, value))
        
        return parsed_args
    
    def __df_to_tablestring(self, df: pd.DataFrame) -> str:
       
        col_widths = [max(df[col].astype(str).str.len().max(), len(col)) for col in df.columns]
        header = '| ' + ' | '.join(f'{col:^{width}}' for col, width in zip(df.columns, col_widths)) + ' |'
        separator = '|-' + '-|-'.join('-' * width for width in col_widths) + '-|'
        rows = []
        for _, row in df.iterrows():
            row_str = '| ' + ' | '.join(f'{str(val):^{width}}' for val, width in zip(row, col_widths)) + ' |'
            rows.append(row_str)

        table_string = '\n'.join([header, separator] + rows)
        return table_string

    def __format_output(self, data: list[dict], output_format: Literal["df", "json", "table", "dict"]):
        if output_format == "json":
            return json.dumps(obj=data, indent=2)
        elif output_format == "df":
            df = pd.DataFrame(data)
            cols = list(df.columns)

            if "id" in cols:
                cols.remove("id")
                df = df[["id"] + cols]

            return df
        elif output_format == "table":
            df = pd.DataFrame(data)
            cols = list(df.columns)
            if "id" in cols:
                cols.remove("id")
                df = df[["id"] + cols]
        
            return self.__df_to_tablestring(df=df)
        return data
    
    def __get_filename(self, path: str, keep_extension=True):
        filename = os.path.basename(path)
        if not keep_extension:
            filename, _ = os.path.splitext(filename)
        return filename

    def __get_file_extension(self, path: str):
        _, ext = os.path.splitext(path)
        return ext  

    
    def __make_request(self, endpoint, method = "POST", **kwargs):
        """
        Private method to handle HTTP requests and errors.
        
        Args:
            method (str): HTTP method ("GET", "POST", "PUT", "DELETE", etc.).
            endpoint (str): API endpoint (e.g., "/catalog/collections/all").
            **kwargs: Additional arguments for `requests.request()` (e.g., `json`, `params`).
        
        Returns:
            dict: Parsed JSON response.
        
        Raises:
            Exception: If the request fails, includes API error details.
        """

        url = f"{self.__lakehouse_url}{endpoint}"
        headers = {
            "Authorization": f"Bearer {self.__access_token}",
            "Content-Type": "application/json",
        }

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                **kwargs
            )
            
            response.raise_for_status()
            
            return response.json()
        
        except requests.exceptions.HTTPError as _:
            try:
                error_detail = response.json().get("detail", response.text)
            except ValueError:
                error_detail = response.text or "No error details provided"
            
            raise Exception(f"API request failed ({response.status_code}): {error_detail}")
        
        except requests.exceptions.RequestException as req_err:
            raise Exception(f"Request failed: {str(req_err)}")
        
        except ValueError as json_err:
            raise Exception(f"Failed to parse API response: {str(json_err)}")
    
    # Authentication function
    def auth(self, email: str, password: str) -> str:
        """Authenticates the user based on the logn details. It returns the authentication token"""

        msg = "Session Authenticated!"

        auth_payload = dict(email=email, password=password)

        response = self.__make_request(method="POST", endpoint="/auth/login", json=auth_payload)

        if response:
            self.__user_id = response["user_id"]
            self.__user_role = response["user_role"]
            self.__access_token = response["access_token"]
            self.__refresh_token = response["refresh_token"]
            self.__access_token_type = response["token_type"]
            self.__user_email = email
        else:
            msg = "Unable to authenticate!"

        return msg
     

    # Creating functions
    def create_collection(
        self,
        storage_type: Storage,
        collection_name: str,
        bucket_name: str,
        collection_description: str = None,
        public: bool = False,
        secret: bool = False
    ) -> str:
        """Description: Created a new collections of file. Returns a collection's name. \n
        Parameters: \n
        - storage_type: is the type of storage that your new collection will be placed at. ('gcp', 's3', 'hdfs')\n
        - collection_name: is the name assigned to the new collection\n
        - namenode address [Optional]: is the ip address (or url) for the namenode if hdfs is the destination storage\n
        - bucket_name [Optional]: is the bucket name (identifier) for the bucket if gpc or s3 are the destination storage\n
        - collection_description [Optional]: optional text description for the collection to be created\n
        - public [Optional]: optional boolean value indicating whether the collection will be public\n
        - secret [Optional]: optional boolean value indicating whether the collection will be secret\n
        """

        payload = {
            "storage_type": storage_type,
            "collection_name": collection_name,
            "public": public,
            "secret": secret
        }

        if collection_description:
            payload["collection_description"] = collection_description

        if storage_type == 'hdfs':
            payload["namenode_address"] = bucket_name
        elif bucket_name: 
            payload["bucket_name"] = bucket_name

        response = self.__make_request(method="POST", endpoint="/storage/collections/create", json=payload)

        print(response)

        return response
    

    # Downloading functions
    def download_file( 
        self,
        catalog_file_id: str,
        output_file_dir: str = None
    ) -> str:
        
        print("Downloading data...")

        catalog_item = self.__make_request(method="GET", endpoint=f"/catalog/file/id/{catalog_file_id}")      

        payload = {
            "catalog_file_id": catalog_file_id
        }

        response = self.__make_request(method="POST", endpoint="/storage/files/download-request", json=payload)
        
        signed_url = response["download_url"]

        if not output_file_dir:
            output_file_dir = os.getcwd()

        output_file_path = os.path.join(output_file_dir, catalog_item['file_name'])
        
        response = requests.get(signed_url, stream=True)

        if response.status_code == 200:
            with open(output_file_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):  # 1  MB
                    if chunk:
                        file.write(chunk)
        else:
            print(f"Failed to download file. Status Code: {response.status_code}, Error: {response.text}")

        print(f"Data downloaded to {output_file_path}")

        return output_file_path


    # Get functions
    def get_dataframe(self, catalogue_file_id: str) -> pd.DataFrame | dict:
        """Description: Get a file as a dataframe. \n
        Condition: the file must be CSV, XLSX, TSV, JSON, MD, HTML, TEX or PARQUET. If the file record's 'file_category' property is marked as 'structured' in the catalogue, the file is can be converted into a dataframe. \n
        Parameters:\n
        - catalogue_file_id: is the id for the dataframe record in the catalog
        """

        downloaded_file_path = self.download_file(
            catalog_file_id=catalogue_file_id,
            output_file_dir=self.__file_load_path
        )

        df = None

        if downloaded_file_path.endswith(".csv"):
            df = pd.read_csv(downloaded_file_path)
        elif downloaded_file_path.endswith(".xlsx") or downloaded_file_path.endswith(".xls"):
            df = pd.read_excel(downloaded_file_path)
        elif downloaded_file_path.endswith(".tsv"):
            df = pd.read_csv(downloaded_file_path, sep="\t")
        elif downloaded_file_path.endswith(".json"):
            df = pd.read_json(downloaded_file_path)
        elif downloaded_file_path.endswith(".md"):
            df = pd.read_csv(downloaded_file_path, delimiter="|", skipinitialspace=True)
        elif downloaded_file_path.endswith(".html"):
            df_list = pd.read_html(downloaded_file_path)  # Returns a list of tables
            df = df_list[0] if df_list else None
        elif downloaded_file_path.endswith(".parquet"):
            df = pd.read_parquet(downloaded_file_path)
        else:
            with open(downloaded_file_path, "r", encoding="UTF-8") as f:
                file_content = f.read()

            pattern = r"[^\\/]+$"

            match = re.search(pattern, downloaded_file_path)

            filename = match.group() if match else None

            parsed_file = dict(
                dataset_name=filename,
                content=file_content
            )

            df = parsed_file
        
        if os.path.exists(downloaded_file_path):
            os.remove(downloaded_file_path)
    
        return df
    
    
    # listing dictionaries functions
    def list_collections(
        self,
        sort_by_key: str = None, 
        sort_desc: bool = False
    ) -> pd.DataFrame:
    # ) -> str:
        """Description: Lists all available collections and returns a string in a table format\n"""
        df = self.list_collections_df(sort_by_key, sort_desc)
        # return self.__df_to_tablestring(df=df)
        return df
    
    def list_files(
        self,
        include_raw: bool = True, 
        include_processed: bool = True, 
        include_curated: bool = True, 
        sort_by_key: str = None, 
        sort_desc: bool = False
    ) -> pd.DataFrame:
    # ) -> str:
        """Description: Lists all available files and returns a string in a table format with the records\n"""
        df  = self.list_files_df(include_raw, include_processed, include_curated, sort_by_key, sort_desc)
        return df
        # return self.__df_to_tablestring(df=df)
      
    def list_buckets(
        self
    ) -> pd.DataFrame:
    # ) -> str:
        """Lists all the available storage buckets in the system and returns a string formated as a table with the records"""
        df = self.list_buckets_df()
        # return self.__df_to_tablestring(df=df)
        return df


    # listing dictionaries functions
    def list_collections_dict(
        self,
        sort_by_key: str = None, 
        sort_desc: bool = False
    ) -> list[dict]:
        """Description: Lists all available collections and returns a list of dictionary with the records\n"""

        response = self.__make_request(method="GET", endpoint="/catalog/collections/all")

        records = response.get("records", [])

        if sort_by_key:
            records = sorted(iterable=records, key=lambda item: item[sort_by_key], reverse=sort_desc)

        return records
    
    def list_files_dict(
        self,
        include_raw: bool = True, 
        include_processed: bool = True, 
        include_curated: bool = True, 
        sort_by_key: str = None, 
        sort_desc: bool = False
    ) -> list[dict]:
        """Description: Lists all available files and returns a list of dictionaries with the records\n"""

        response = self.__make_request(method="GET", endpoint="/catalog/files/all")

        records = response.get("records", [])

        if sort_by_key:
            records = sorted(iterable=records, key=lambda item: item[sort_by_key], reverse=sort_desc)

        filter_options = []

        if include_raw:
            filter_options.append("raw")
        if include_processed:
            filter_options.append("processed")
        if include_curated:
            filter_options.append("curated")

        filtered_response = [
            item for item in records if item["processing_level"] in filter_options
        ]

        return filtered_response

    def list_buckets_dict(self) -> list[dict]:
        """Lists all the available storage buckets in the system and returns a list of dictionaries with the records"""

        response = self.__make_request(method="GET", endpoint="/storage/bucket-list")
        
        response = dict(response)

        bucket_list = list(response.get("bucket_list", []))

        sorted_list = sorted(bucket_list, key=lambda item: item["bucket_name"]) if bucket_list else []

        return sorted_list


    # listing json functions
    def list_collections_json(
        self,
        sort_by_key: str = None, 
        sort_desc: bool = False,
        indented: bool = True
    ) -> str:
        """Description: Lists all available collections and returns a formatted json string with the records\n"""

        records = self.list_collections_dict(sort_by_key, sort_desc)

        records = json.dumps(obj=records,indent=2) if indented else json.dumps(obj=records)

        return records
    
    def list_files_json(
        self,
        include_raw: bool = True, 
        include_processed: bool = True, 
        include_curated: bool = True, 
        sort_by_key: str = None, 
        sort_desc: bool = False,
        indented: bool = True
    ) -> str:
        """Description: Lists all available files and returns a formatted json string with the records\n"""

        records = self.list_files_dict(include_raw, include_processed, include_curated, sort_by_key, sort_desc)

        records = json.dumps(obj=records,indent=2) if indented else json.dumps(obj=records)

        return records
    
    def list_buckets_json(self, indented: bool = True) -> str:
        """Lists all the available storage buckets in the system and returns a json string with the records"""

        records = self.list_buckets_dict()

        records = json.dumps(obj=records,indent=2) if indented else json.dumps(obj=records)

        return records


    # listing df functions
    def list_collections_df(
        self,
        sort_by_key: str = None, 
        sort_desc: bool = False
    ) -> pd.DataFrame:
        """Description: Lists all available collections and returns a dataframe with the records\n"""

        records  = self.list_collections_dict(sort_by_key, sort_desc)

        df = pd.DataFrame(records)

        columns_order = ["id", "collection_name", "inserted_by", "inserted_at", "public"]

        filtered_df = df[columns_order].copy()

        filtered_df["inserted_at"] = pd.to_datetime(filtered_df["inserted_at"], unit='s').dt.strftime('%Y-%m-%d')

        filtered_df["inserted_by"] = filtered_df["inserted_by"].str.split(":").str[1]

        return filtered_df
    
    def list_files_df(
        self,
        include_raw: bool = True, 
        include_processed: bool = True, 
        include_curated: bool = True, 
        sort_by_key: str = None, 
        sort_desc: bool = False
    ) -> pd.DataFrame:
        """Description: Lists all available files and returns a dataframe with the records\n"""

        records  = self.list_files_dict(include_raw, include_processed, include_curated, sort_by_key, sort_desc)

        df = pd.DataFrame(records)

        columns_order = ["id", "file_name", "file_category", "file_size", "processing_level", "public", "inserted_by", "inserted_at", "collection_id", "collection_name"]

        filtered_df = df[columns_order].copy()

        filtered_df["inserted_at"] = pd.to_datetime(filtered_df["inserted_at"], unit='s').dt.strftime('%Y-%m-%d')

        filtered_df["inserted_by"] = filtered_df["inserted_by"].str.split(":").str[1]

        filtered_df["file_size"] = filtered_df["file_size"].apply(lambda size: self.__format_size(int(size)))

        return filtered_df
      
    def list_buckets_df(self) -> pd.DataFrame:
        """Lists all the available storage buckets in the system and returns a dataframe with the records"""

        records = self.list_buckets_dict()

        df = pd.DataFrame(records)

        return df

    # upload function
    def upload_dataframe(
        self,
        df: pd.DataFrame,
        df_name: str,
        collection_catalog_id: str,
        dataframe_description: str = "",
        dataframe_version: int = 1,
        public: bool = False,
        processing_level: Literal["raw", "processed", "curated"] = "raw"
    )-> tuple[str, str, str]:
        """Description: Set up a new file to be uploaded from local storage. It returns the upload token, the credential id and the local dataframe file path to upload it.\n
         Parameters:\n
        - df: the dataframe that should be uploaded
        - df_name: the dataframe name only, without the extension. By default the dataframe will be stored as a CSV file\n
        - collection_catalog_id: the collection identifiyer, from the collection catalog, where the file will be placed in\n
        - file_description [Optional]: Additional description for the file\n
        - dataframe_version [Optional, default 1]: The version of this dataframe in the system \n
        - public [Optional, default False]: The visibility of the dataframe, if public all users can see in the catalog
        - processing_level [Optional, default raw]: The processing level of this dataframe
        """      

        # saving dataframe into csv
        df_file_path = f"{self.__file_load_path}/{df_name}.csv"

        df.to_csv(df_file_path)

        upload_response = self.upload_file(
            local_file_path=df_file_path,
            final_file_name=f"{df_name}.csv",
            collection_catalog_id=collection_catalog_id,
            file_category="structured",
            file_description=dataframe_description,
            file_version=dataframe_version,
            public=public,
            processing_level=processing_level
        )

        if os.path.exists(df_file_path):
            os.remove(df_file_path)

        return upload_response

    def upload_file(
        self,
        local_file_path: str,
        final_file_name: str,
        collection_catalog_id: str,
        file_category: Literal["structured", "unstructured"] = "unstructured",
        file_description: str = None,
        file_version: int = 1,
        file_size: int = 0,
        public: bool = False,
        processing_level: Literal["raw", "processed", "curated"] = "raw"
    ) -> dict[str]:
        """Description: Set up a new file to be uploaded from local storage. It returns the catalog item for the new file uploaded.\n
         Parameters:\n
        - local_file_path: the local path to the file to be uploaded\n
        - final_file_name: the output file name in the storage\n
        - collection_catalog_id: the collection identifiyer, from the collection catalog, where the file will be placed in\n
        - file_category: the file class must indicate if the file is 'structured' or 'unstructured', by default the file is set to be 'unstructured'. Structured files can be Columnar or document files such as csv, tsv, excel, json, parquet. \n
        - collection_id: the collection identifiyer, from the collection catalog, where the file will be placed in\n
        - file_description [Optional]: Additional description for the file\n
        - file_version [Optional, default 1]: The version of the file you are uploading for version control
        - file_size [Optional, default 0]: The size of the file you are uploading in bytes, the sizen will be taken by default from your system
        - public [Optional, default False]: The visibility of the dataframe, if public all users can see in the catalog
        - processing_level [Optional]: A string containing the processing level of the file to be uploaded, e.g., ["raw", "processed", "curated"]\n
        """  

        print("Uploading data...")

        file_size = os.path.getsize(local_file_path)

        file_extension = self.__get_file_extension(local_file_path)

        if file_extension:
            final_file_name_lower = final_file_name.lower()
            file_extension_lower = file_extension.lower()
            
            if not final_file_name_lower.endswith(file_extension_lower):
                final_file_name += file_extension
        
        payload = {
            "collection_catalog_id": collection_catalog_id,
            "file_name": final_file_name,
            "file_category": file_category,
            "file_version": file_version,
            "file_size": file_size,
            "public": public,
            "processing_level": processing_level,
            "file_description": file_description
        }

        response = self.__make_request(method="POST", endpoint="/storage/files/upload-request", json=payload)

        signed_url = response["upload_url"]

        catalog_record_id = response["catalog_record_id"]

        method = str(response["method"])

        CHUNK_SIZE = 10 * 1024 * 1024

        if method.lower() == "put":
            with open(local_file_path, "rb") as file:
                while chunk := file.read(CHUNK_SIZE):
                    response = requests.put(signed_url, data=chunk, headers={"Content-Type": "application/octet-stream"})
                    response.raise_for_status()
        else:
            with open(local_file_path, 'rb') as file:
                while chunk := file.read(CHUNK_SIZE):
                    response = requests.post(signed_url, data=chunk, headers={'Content-Type': 'application/octet-stream'})
                    response.raise_for_status()
     
        payload = {"status": "ready"}

        response = self.__make_request(method="PUT", endpoint=f"/catalog/set-file-status/{catalog_record_id}", json=payload)

        print("Data uploaded!")

        return response


    # search function
    def search_collections_by_keyword(
        self,
        keyword: str,
        output_format: Literal["df", "json", "dict", "table"] = "table"
    ) -> dict:
        """Description: Search files on the catalogue based on the given filters\n
            Parameter: \n
            - keyword: A string containing the keyword to search for, the search will match the collection names to the keyword
            - output_format: A string containing one of the following options ["df", "json", "dict", "table"]
        """

        if output_format not in ["df", "json", "dict", "table"]:
            raise Exception("Must specify output format")

        filters = [
            CatalogFilter(
                property_name="collection_name",
                operator="*",
                property_value=keyword
            ) 
        ]

        try:
            payload = CatalogFilterPayload(filters=filters)
        except Exception:
            raise Exception("Incorrect filter format!")

        dumped_payload = payload.model_dump()

        response = self.__make_request(method="POST", endpoint="/catalog/collections/search", json=dumped_payload)

        records = response.get("records", [])

        records = self.__format_output(data=records, output_format=output_format)
      
        return records
    
    def search_files_by_keyword(
        self,
        keyword: str,
        output_format: Literal["df", "json", "dict", "table"] = "table"
    ) -> dict:
        """Description: Search files on the catalogue based on the given filters\n
            Parameter: \n
            - keyword: A string containing the keyword to search for, the search will match the file names to the keyword
            - output_format: A string containing one of the following options ["df", "json", "dict", "table"]
        """

        if output_format not in ["df", "json", "dict", "table"]:
            raise Exception("Must specify output format")

        filters = [
            CatalogFilter(
                property_name="file_name",
                operator="*",
                property_value=keyword
            ) 
        ]

        try:
            payload = CatalogFilterPayload(filters=filters)
        except Exception:
            raise Exception("Incorrect filter format!")

        dumped_payload = payload.model_dump()

        response = self.__make_request(method="POST", endpoint="/catalog/files/search", json=dumped_payload)

        records = response.get("records", [])

        records = self.__format_output(data=records, output_format=output_format)
      
        return records
    
    
    def search_collections_query(
        self,
        *args,
        output_format: Literal["df", "json", "dict", "table"] = "table"
    ) -> list[dict]:
        """Description: Search files on the catalogue based on the given filters\n
            
            Parameters:
                - output_format (str): A string containing one of the following options ["df", "json", "dict", "table"]
                args: string containing the search terms \n

            Arguments: 
                Must be query strings containing the search terms such as "collection_name=lakehouse" \n

            Query string structure: 
                KEY[OPERATOR]VALUE. Operator can be "=", ">","<", ">=", "<=" or the wildcard operator "*" (substring match)

            Usage example: 
                search_collections_query('collection_name*lake','inserted_by=user1@gmail.com','inserted_at>1747934722', 'public=True', output_format='table')
        """

        if output_format not in ["df", "json", "dict", "table"]:
            raise Exception("Must specify output format")

        parsed_args = self.__parse_query_args(args=args)

        filters = [
            CatalogFilter(
                property_name=key,
                operator=op,
                property_value=value
            ) 
            for (key, op, value) in parsed_args
        ]

        try:
            payload = CatalogFilterPayload(filters=filters)
        except Exception:
            raise Exception("Incorrect filter format!")

        parsed_payload = payload.model_dump()

        response = self.__make_request(method="POST", endpoint="/catalog/collections/search", json=parsed_payload)

        records = response.get("records", [])

        records = self.__format_output(data=records, output_format=output_format)
      
        return records
    

    def search_files_query(
        self,
        *args,
        output_format: Literal["df", "json", "dict", "table"] = "table"
    ) -> list[dict]:
        """Description: Search files on the catalogue based on the given filters\n
            Parameters:
                - output_format (str): A string containing one of the following options ["df", "json", "dict", "table"]
                args: string containing the search terms\n

            Arguments: 
                Must be query strings containing the search terms such as "collection_name=lakehouse"\n

             Query string structure: 
                KEY[OPERATOR]VALUE. Operator can be "=", ">","<", ">=", "<=" or the wildcard operator "*" (substring match)

            Usage example: 
                search_files_query('file_name*sample','inserted_by=user1@gmail.com','inserted_at>1747934722', 'public=True', output_format='table')
        """

        if output_format not in ["df", "json", "dict", "table"]:
            raise Exception("Must specify output format")

        parsed_args = self.__parse_query_args(args=args)

        filters = [
            CatalogFilter(
                property_name=key,
                operator=op,
                property_value=value
            ) 
            for (key, op, value) in parsed_args
        ]

        try:
            payload = CatalogFilterPayload(filters=filters)
        except Exception:
            raise Exception("Incorrect filter format!")

        parsed_payload = payload.model_dump()

        response = self.__make_request(method="POST", endpoint="/catalog/files/search", json=parsed_payload)

        records = response.get("records", [])

        records = self.__format_output(data=records, output_format=output_format)
      
        return records

