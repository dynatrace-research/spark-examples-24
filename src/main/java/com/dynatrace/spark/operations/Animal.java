package com.dynatrace.spark.operations;

import com.google.gson.annotations.SerializedName;

public class Animal {
  public enum Species {
    @SerializedName("BIRD")
    BIRD,
    @SerializedName("CAT")
    CAT,
    @SerializedName("DOG")
    DOG,
    @SerializedName("FISH")
    FISH,
    @SerializedName("HAMSTER")
    HAMSTER;
  }

  private String name;
  @SerializedName("species")
  private Species species;
  private String color;
  private int age;

  public Animal() {
  }

  public Animal(String name, Species species, String color, int age) {
    this.name = name;
    this.species = species;
    this.color = color;
    this.age = age;
  }

  public String getName() {
    return name;
  }

  public Species getSpecies() {
    return species;
  }

  public String getColor() {
    return color;
  }

  public int getAge() {
    return age;
  }

}