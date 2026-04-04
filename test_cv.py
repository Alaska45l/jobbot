import asyncio
from utils.cv_builder import compilar_cv_dinamico

async def main():
    print("Compilando CV de prueba...")
    
    # Simulamos que el bot encontró una empresa y estas keywords
    pdf_bytes = await compilar_cv_dinamico(
        nombre_empresa="Tech & Security MdP", 
        keywords=["Ciberseguridad", "Auditoría de APIs", "Soporte IT", "Python"]
    )
    
    # Guardamos el resultado en disco para poder abrirlo
    with open("CV_Prueba_Visual.pdf", "wb") as f:
        f.write(pdf_bytes)
        
    print("¡Listo! Abrí el archivo 'CV_Prueba_Visual.pdf' para ver cómo quedó.")

if __name__ == "__main__":
    asyncio.run(main())