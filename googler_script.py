from urllib.parse import urlencode, quote_plus
from bs4 import BeautifulSoup
from collections import Counter
import re
import requests
import pdfkit
import fitz  # PyMuPDF
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlencode, quote_plus
from bs4 import BeautifulSoup
from collections import Counter
import re
import requests
import pdfkit
import fitz  # PyMuPDF
import pandas as pd  # Assuming you need pandas for DataFrame  
import aiohttp
import asyncio
import nest_asyncio
nest_asyncio.apply()
print("new googler")
class googler:
    def generate_google_search_url(
        self,
        queries ,
        page: int = 1,
        language: str = 'en',
        country: str = 'US',
        time_range: str = '',  # Examples: 'd' for past 24 hours, 'w' for past week, 'm' for past month, 'y' for past year
        sort_by_date: bool = False,
        safe_search: bool = True,
        file_type: str = '',  # Example: 'pdf', 'doc', 'ppt'
        site: str = '',  # Restrict search to a specific site, e.g., 'example.com'
    ):
        """
        Generates a Google search URL with optional filters.

        :param query: Search query string.
        :param page: Page number of the results (1-indexed).
        :param language: Language code (e.g., 'en' for English).
        :param country: Country code (e.g., 'US' for the United States).
        :param time_range: Time range filter ('d', 'w', 'm', 'y').
        :param sort_by_date: Whether to sort results by date.
        :param safe_search: Enable SafeSearch.
        :param file_type: Restrict results to a specific file type (e.g., 'pdf').
        :param site: Restrict results to a specific site.
        :return: URL string for Google search with the specified filters.
        """
        base_url = 'https://www.google.com/search'
        cmb_data = []
        for query in queries:
            # Construct search parameters
            params = {
                'q': query,
                'hl': language,
                'gl': country,
                'start': (page - 1) * 10,  # Calculate the starting result index for pagination (10 results per page)
                'safe': 'active' if safe_search else 'off',
            }

            # Time range filter
            if time_range:
                params['tbs'] = f'qdr:{time_range}'

            # Sort by date
            if sort_by_date:
                params['tbs'] = params.get('tbs', '') + ',sbd:1'

            # File type filter
            if file_type:
                params['as_filetype'] = file_type

            # Site-specific search
            if site:
                params['q'] = f'site:{site} {params["q"]}'

            # Encode parameters
            encoded_params = urlencode(params, quote_via=quote_plus)
            full_url = f"{base_url}?{encoded_params}"
            cmb_data.append([query,full_url])
        return cmb_data





    def download_html(self,url):

        # Set up headers to mimic a browser request
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36"
        }

        # Make a GET request to fetch the raw HTML content
        response = requests.get(url, headers=headers)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the content of the request with BeautifulSoup
            soup = BeautifulSoup(response.text, "html.parser")

            # Print the prettified HTML of the page
        # print(soup.prettify())
            return soup

            # Save the HTML content to a file
            with open("google_search_results.html", "w", encoding='utf-8') as file:
                file.write(soup.prettify())
        else:
            print("Failed to retrieve the webpage. Status code:", response.status_code)
            return "No Content found"


    def remove_elements(self,html_string, tags_to_remove=None, classes_to_remove=None):
        """
        Removes specific tags and classes from the HTML string and returns the cleaned HTML.
        
        Parameters:
        html_string (str): The input HTML string.
        tags_to_remove (list): A list of tag names to remove.
        classes_to_remove (list): A list of class names to remove.
        
        Returns:
        str: The cleaned HTML string.
        """
    # print(html_string[0:500])
        # Parse HTML string with BeautifulSoup
        soup = BeautifulSoup(html_string, 'html.parser')
        
        # Remove specific tags
        if tags_to_remove:
            for tag in tags_to_remove:
                for element in soup.find_all(tag):
                    element.decompose()  # Completely remove the tag from the tree
        
        # Remove specific classes
        if classes_to_remove:
            for class_name in classes_to_remove:
                for element in soup.find_all(class_=class_name):
                    element.decompose()  # Completely remove the element with the class from the tree
        
        # Return cleaned HTML as string
        return str(soup)

    def parse_google_search_results(self,html_content):
        soup = BeautifulSoup(html_content, 'html.parser')

        # This will hold the results
        results = {'rank':[],'title':[],'description':[],'link':[]}

        # Google search results often contain <a> tags where each result is a child
        # We look for <a> tags that contain <h3> tags for titles
        search_results = soup.find_all('a')

        rm = 1
        # Iterate over each <a> tag to filter relevant search results
        for rank, result in enumerate(search_results, start=1):
            # Find the title
            title_tag = result.find('h3')
            if title_tag:
                # Extract title
                title = title_tag.get_text(strip=True)

                # Extract link
                link = result['href']
                # Clean the link if it contains the '/url?q=' pattern
                if link.startswith('/url?q='):
                    link = link.split('/url?q=')[1].split('&')[0]
                
                # Find the description: it's typically the next <div> or <span> after the title <h3> tag in the HTML structure
                description_tag = result.find_next('div', class_=lambda x: x is None)
                if description_tag:
                    description = description_tag.get_text(strip=True)
                else:
                    description = "No description"

                # Append the data to the results
                results['rank'].append(rm)
                results['title'].append(title)
                results['description'].append(description)
                results['link'].append(link)
                rm = rm+1

        results_df = pd.DataFrame(results)
        return results_df

    async def fetch_html(self, session, url):
        try:
            async with session.get(url) as response:
                # Check for successful response
                if response.status == 200:
                    content_type = response.headers.get('Content-Type', '').lower()
                    
                    # Determine if content is downloadable (not text-based)
                    if 'text/html' in content_type:
                        return (await response.text(), "text/html")
                    else:
                        return (await response.read(), "non text/html")  # Return bytes for downloadable content
                else:
                    return (f"Error: Received status code {response.status}", "text/html")  # Return error message if response status is not OK
        except Exception as e:
            return (f"Error: {str(e)}", "text/html")  # Return error message if there's an exception


    async def fetch_all(self, urls):
        async with aiohttp.ClientSession(headers={'User-Agent': 'Mozilla/5.0'}) as session:
            tasks = [self.fetch_html(session, url) for url in urls]
            return await asyncio.gather(*tasks)

    def fetch_async_html_responses(self,cmb_data_or):
        cmb_data = cmb_data_or.copy()

        all_urls = []
        for cd in cmb_data:
            all_urls.append(cd[1])
        # Running the asyncio event loop
        all_urls_htmls = asyncio.run(self.fetch_all(all_urls))
        
        for cd,auh,au in zip(cmb_data,all_urls_htmls,all_urls):
            if cd[1]!=au:
                print("Some issue here")
            else:
                auh_content = auh[0]
                auh_type = auh[1]
                cd.append(auh_content)
                
                if auh_content.startswith("Error"):  
                    cd.append('0')
                else:
                    cd.append('1')
                cd.append(auh_type)
        #query, url, html, 0/1,type
        return cmb_data

    def concater(self,dfs,axis=1):
        dfs_new = []
        for df in dfs:
            df.reset_index(inplace=True,drop=True)
            dfs_new.append(df)
        op = pd.concat(dfs_new,axis=axis)
        return op

    def fetch_results(
        self,
            queries,
            page: int = 1,
            language: str = 'en',
            country: str = 'US',
            time_range: str = '',  # Examples: 'd' for past 24 hours, 'w' for past week, 'm' for past month, 'y' for past year
            sort_by_date: bool = False,
            safe_search: bool = True,
            file_type: str = '',  # Example: 'pdf', 'doc', 'ppt'
            site: str = ''):
            # Example usage:
        cmb_data = self.generate_google_search_url(
            queries,
            page,
            language,
            country,
            time_range,  # Results from the past week
            sort_by_date,
            safe_search,
            file_type,
            site)

        cmd_data = self.fetch_async_html_responses(cmb_data)

        urls_list_with_failed_fetch = []
        urls_list_with_success_fetch = []
        dfs = []
        for cd in cmd_data:
        #query, url, html, 0/1,type
            if cd[3]=="1":
                html_content_cleaned = self.remove_elements(cd[2], tags_to_remove=['script','style'])
               # print(html_content_cleaned)
                df = self.parse_google_search_results(html_content_cleaned)
                #print(df)
                df['query'] =cd[0]
                df['query_url']=cd[1]
                df['success']=cd[3]
                df['type']=cd[4]
                dfs.append(df)
                urls_list_with_success_fetch.append(cd)
            else:
                urls_list_with_failed_fetch.append(cd)

        dfs = self.concater(dfs,axis=0)
        return dfs,urls_list_with_failed_fetch,urls_list_with_success_fetch 




