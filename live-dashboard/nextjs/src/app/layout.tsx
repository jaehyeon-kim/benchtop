import type { Metadata } from "next";
import "./globals.css";
import Providers from "@/app/providers";

export const metadata: Metadata = {
  title: "theLook eCommerce Dashboard",
  description: "A demo dashboard theLook eCommerce",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>
        <div className="container mx-auto max-w-8xl">
          <Providers>{children}</Providers>
        </div>
      </body>
    </html>
  );
}
