package br.unifal.redes;

import br.unifal.redes.sender.network.WindowManager;

public class TestWindowManager {

    public static void main(String[] args) {

        WindowManager window =
                new WindowManager(4);

        System.out.println(window);

        int s0 = window.packetSent();
        int s1 = window.packetSent();

        System.out.println("Enviados:");
        System.out.println(s0);
        System.out.println(s1);

        System.out.println(window);

        window.processAck(0);

        System.out.println(window);
    }
}