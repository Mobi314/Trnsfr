from PIL import Image
import os

def images_to_pdf(image_folder, output_pdf):
    # Get all image file paths in the folder
    image_files = [os.path.join(image_folder, f) for f in os.listdir(image_folder) if f.endswith(('png', 'jpg', 'jpeg', 'bmp', 'gif'))]
    
    # Sort images by name to maintain order
    image_files.sort()
    
    # Open the first image and convert it to RGB (necessary for PDF conversion)
    first_image = Image.open(image_files[0]).convert('RGB')
    
    # Open all subsequent images and convert them to RGB
    other_images = [Image.open(image).convert('RGB') for image in image_files[1:]]
    
    # Save all images into a single PDF
    first_image.save(output_pdf, save_all=True, append_images=other_images)
    
    print(f"PDF saved successfully as {output_pdf}")

# Example usage
image_folder = 'path_to_your_image_folder'  # Replace with your image folder path
output_pdf = 'output.pdf'  # Replace with your desired output PDF name

images_to_pdf(image_folder, output_pdf)
