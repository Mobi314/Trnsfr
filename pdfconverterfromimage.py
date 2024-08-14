from PIL import Image

def images_to_pdf(image_files, output_pdf):
    # Check if the list of image files is not empty
    if not image_files:
        print("No image files provided.")
        return
    
    # Ensure all image files exist before proceeding
    for image in image_files:
        if not os.path.isfile(image):
            print(f"The file {image} does not exist.")
            return
    
    # Sort images by name to maintain order (optional)
    image_files.sort()
    
    # Log the images being processed
    print(f"Processing the following images: {image_files}")
    
    try:
        # Open the first image and convert it to RGB (necessary for PDF conversion)
        first_image = Image.open(image_files[0]).convert('RGB')
        
        # Open all subsequent images and convert them to RGB
        other_images = [Image.open(image).convert('RGB') for image in image_files[1:]]
        
        # Save all images into a single PDF
        first_image.save(output_pdf, save_all=True, append_images=other_images)
        
        print(f"PDF saved successfully as {output_pdf}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage
image_files = [
    'image1.jpg',  # Replace with your actual image file names
    'image2.png',
    'image3.jpeg'
]
output_pdf = 'output.pdf'  # Replace with your desired output PDF name

images_to_pdf(image_files, output_pdf)
