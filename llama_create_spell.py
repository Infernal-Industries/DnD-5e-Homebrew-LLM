import ollama

# Load the LLaMA 3.1 model
model = ollama.load('llama-3.1')

# Read the spell data
with open('spell_details_compact.txt', 'r', encoding='utf-8') as file:
    spell_data = file.read()

# Construct a prompt to create a new spell
prompt = f"""
Based on the comprehensive DnD 5e spell data provided, create a new and unique spell. 
Include details like those in the spells provided. 
Use features from existing spells to inspire the new spell:

{spell_data}
"""

# Run the model with the prompt
response = model(prompt)
new_spell = response['choices'][0]['text']

# Output the generated spell
print("Generated New Spell:")
print(new_spell)