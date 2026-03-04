# 🌸 Ceibo – Gestión de Rutas

Una aplicación web sencilla para registrar y gestionar rutas, construida con HTML, CSS y JavaScript.

## ✨ Funcionalidades

- Agregar rutas con nombre, origen, destino y distancia.
- Eliminar rutas con un clic.
- Los datos se guardan automáticamente en el navegador (localStorage).

## 🚀 Publicar con GitHub Pages

La aplicación se publica automáticamente en **GitHub Pages** cada vez que se hace un `push` a la rama `main`.

### Pasos para activar GitHub Pages

1. Ve a **Settings → Pages** en tu repositorio de GitHub.
2. En **Source**, selecciona **GitHub Actions**.
3. Haz un `push` a `main` — el workflow se encargará del resto.

La URL pública tendrá el formato:
```
https://<tu-usuario>.github.io/<nombre-del-repositorio>/
```

## 🖥️ Uso local

Abre `index.html` directamente en tu navegador o usa la extensión **Live Server** de VS Code:

1. Instala la extensión [Live Server](https://marketplace.visualstudio.com/items?itemName=ritwickdey.LiveServer).
2. Haz clic derecho en `index.html` → **Open with Live Server**.

## 📁 Estructura del proyecto

```
ceibo/
├── index.html          # Estructura de la página
├── style.css           # Estilos
├── app.js              # Lógica de la aplicación
└── .github/
    └── workflows/
        └── deploy.yml  # Workflow de despliegue en GitHub Pages
```
