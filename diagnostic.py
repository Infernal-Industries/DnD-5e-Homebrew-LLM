import tiktoken

# Initialize the tokenizer
encoder = tiktoken.get_encoding("cl100k_base")

# Define a function to read and process the file in chunks
def calculate_token_count(file_path, chunk_size=1024 * 1024):
    total_tokens = 0
    with open(file_path, 'r') as file:
        while True:
            # Read a chunk of the file
            chunk = file.read(chunk_size)
            if not chunk:
                break
            
            # Encode the chunk to count tokens
            tokens = encoder.encode(chunk)
            total_tokens += len(tokens)
    
    return total_tokens

# Calculate the total number of tokens
file_path = "spell_details_compact.txt"
token_count = calculate_token_count(file_path)
print(f"Total tokens in spell_details_compact.txt: {token_count}")
